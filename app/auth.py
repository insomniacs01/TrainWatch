import hashlib
import hmac
import secrets
from dataclasses import asdict, dataclass
from datetime import timedelta
from typing import Any, Dict, Optional

from .config import ServerConfig
from .storage import SQLiteStore
from .time_utils import format_utc, parse_utc_timestamp, utc_now, utc_now_iso


ROLE_VIEWER = "viewer"
ROLE_OPERATOR = "operator"
ROLE_ADMIN = "admin"
ROLE_RANK = {
    ROLE_VIEWER: 10,
    ROLE_OPERATOR: 20,
    ROLE_ADMIN: 30,
}
PASSWORD_ITERATIONS = 200_000


def _password_hash(password: str, salt: Optional[str] = None) -> str:
    actual_salt = salt or secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        str(password or "").encode("utf-8"),
        actual_salt.encode("utf-8"),
        PASSWORD_ITERATIONS,
    ).hex()
    return f"pbkdf2_sha256${PASSWORD_ITERATIONS}${actual_salt}${digest}"


def verify_password(password: str, password_hash: str) -> bool:
    try:
        algorithm, iterations, salt, expected = str(password_hash or "").split("$", 3)
    except ValueError:
        return False
    if algorithm != "pbkdf2_sha256":
        return False
    candidate = hashlib.pbkdf2_hmac(
        "sha256",
        str(password or "").encode("utf-8"),
        salt.encode("utf-8"),
        int(iterations),
    ).hex()
    return hmac.compare_digest(candidate, expected)


@dataclass
class AuthPrincipal:
    username: str
    role: str
    source: str = "session"
    display_name: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def has_role(self, required_role: str) -> bool:
        if self.source == "public":
            return True
        return ROLE_RANK.get(self.role, 0) >= ROLE_RANK.get(required_role, 0)


class AuthManager:
    def __init__(self, store: SQLiteStore, server_config: ServerConfig) -> None:
        self.store = store
        self.server_config = server_config
        self._bootstrap_admin()

    @property
    def user_auth_enabled(self) -> bool:
        return bool(self.server_config.enable_user_auth or self.server_config.bootstrap_admin_username)

    @property
    def auth_required(self) -> bool:
        return bool(self.server_config.shared_token or self.user_auth_enabled)

    @property
    def bootstrap_required(self) -> bool:
        return bool(self.server_config.enable_user_auth and self.store.user_count() == 0)

    @property
    def mode(self) -> str:
        if self.server_config.enable_user_auth:
            return "team"
        if self.server_config.shared_token:
            return "personal-token"
        return "personal"

    def public_principal(self) -> AuthPrincipal:
        return AuthPrincipal(username="public", role=ROLE_VIEWER, source="public", display_name="Public")

    def _bootstrap_admin(self) -> None:
        username = str(self.server_config.bootstrap_admin_username or "").strip()
        password = str(self.server_config.bootstrap_admin_password or "").strip()
        if not username or not password:
            return
        existing = self.store.get_user_record(username)
        if existing:
            return
        now_value = utc_now_iso()
        self.store.upsert_user(
            username=username,
            password_hash=_password_hash(password),
            role=ROLE_ADMIN,
            display_name=username,
            disabled=False,
            created_at=now_value,
            updated_at=now_value,
        )

    def authenticate_token(self, token: str) -> Optional[AuthPrincipal]:
        actual_token = str(token or "").strip()
        if not actual_token:
            return None
        if self.server_config.shared_token and hmac.compare_digest(actual_token, self.server_config.shared_token):
            return AuthPrincipal(username="shared-token", role=ROLE_ADMIN, source="shared_token", display_name="Shared Token")
        session = self.store.get_session(actual_token)
        if not session:
            return None
        return AuthPrincipal(
            username=str(session.get("username", "")).strip() or "user",
            role=str(session.get("role", ROLE_VIEWER)).strip() or ROLE_VIEWER,
            source="session",
            display_name=str(session.get("display_name", "")).strip(),
        )

    def require_token(self, token: str) -> AuthPrincipal:
        principal = self.authenticate_token(token)
        if principal is not None:
            return principal
        if self.auth_required:
            raise PermissionError("Authentication token required")
        return self.public_principal()

    def authenticate_password(self, username: str, password: str) -> Optional[AuthPrincipal]:
        record = self.store.get_user_record(username)
        if not record or bool(record.get("disabled")):
            return None
        if not verify_password(password, str(record.get("password_hash", ""))):
            return None
        return AuthPrincipal(
            username=str(record.get("username", username)),
            role=str(record.get("role", ROLE_VIEWER)) or ROLE_VIEWER,
            source="session",
            display_name=str(record.get("display_name", username)).strip(),
        )

    def create_session(self, principal: AuthPrincipal) -> Dict[str, Any]:
        now = utc_now()
        expires_at = now + timedelta(hours=max(1, int(self.server_config.session_ttl_hours or 24)))
        token = secrets.token_urlsafe(32)
        payload = {
            "token": token,
            "username": principal.username,
            "role": principal.role,
            "display_name": principal.display_name or principal.username,
            "created_at": format_utc(now),
            "last_seen_at": format_utc(now),
            "expires_at": format_utc(expires_at),
        }
        self.store.create_session(payload)
        return payload

    def login(self, username: str, password: str) -> Dict[str, Any]:
        principal = self.authenticate_password(username, password)
        if principal is None:
            raise PermissionError("Invalid username or password")
        session = self.create_session(principal)
        return {
            "token": session["token"],
            "expires_at": session["expires_at"],
            "user": principal.to_dict(),
        }

    def logout(self, token: str) -> None:
        actual_token = str(token or "").strip()
        if actual_token:
            self.store.delete_session(actual_token)

    def assert_role(self, principal: AuthPrincipal, required_role: str) -> None:
        if not principal.has_role(required_role):
            raise PermissionError("Insufficient permissions")

    def list_users(self) -> list:
        return self.store.list_users()

    def create_or_update_user(
        self,
        username: str,
        password: Optional[str],
        role: str,
        display_name: str,
        disabled: bool = False,
    ) -> Dict[str, Any]:
        username = str(username or "").strip().lower()
        if not username:
            raise ValueError("username is required")
        if role not in ROLE_RANK:
            raise ValueError("role must be viewer, operator, or admin")
        existing = self.store.get_user_record(username)
        now_value = utc_now_iso()
        created_at = str(existing.get("created_at", now_value)) if existing else now_value
        if existing:
            password_hash = str(existing.get("password_hash", ""))
            if password:
                password_hash = _password_hash(password)
        else:
            if not password:
                raise ValueError("password is required when creating a user")
            password_hash = _password_hash(password)
        self.store.upsert_user(
            username=username,
            password_hash=password_hash,
            role=role,
            display_name=str(display_name or username).strip() or username,
            disabled=bool(disabled),
            created_at=created_at,
            updated_at=now_value,
        )
        record = self.store.get_user_record(username) or {}
        return {
            "username": username,
            "role": str(record.get("role", role)),
            "display_name": str(record.get("display_name", display_name or username)),
            "disabled": bool(record.get("disabled", disabled)),
            "created_at": str(record.get("created_at", created_at)),
            "updated_at": str(record.get("updated_at", now_value)),
        }

    def bootstrap_admin(self, username: str, password: str, display_name: str = "") -> Dict[str, Any]:
        if not self.server_config.enable_user_auth:
            raise ValueError("Team mode is not enabled")
        if self.store.user_count() > 0:
            raise ValueError("Initial admin already exists")
        return self.create_or_update_user(
            username=username,
            password=password,
            role=ROLE_ADMIN,
            display_name=display_name or username,
            disabled=False,
        )

    def session_summary(self, token: str) -> Optional[Dict[str, Any]]:
        actual_token = str(token or "").strip()
        if not actual_token:
            return None
        if self.server_config.shared_token and hmac.compare_digest(actual_token, self.server_config.shared_token):
            principal = AuthPrincipal(username="shared-token", role=ROLE_ADMIN, source="shared_token", display_name="Shared Token")
            return {"token_source": "shared_token", "user": principal.to_dict()}
        session = self.store.get_session(actual_token)
        if not session:
            return None
        return {
            "token_source": "session",
            "expires_at": str(session.get("expires_at", "")),
            "last_seen_at": str(session.get("last_seen_at", "")),
            "user": {
                "username": str(session.get("username", "")).strip(),
                "role": str(session.get("role", ROLE_VIEWER)).strip() or ROLE_VIEWER,
                "source": "session",
                "display_name": str(session.get("display_name", "")).strip(),
            },
        }

    def session_expired(self, session: Dict[str, Any]) -> bool:
        expires_at = str(session.get("expires_at", "")).strip()
        if not expires_at:
            return True
        return parse_utc_timestamp(expires_at) <= utc_now()
