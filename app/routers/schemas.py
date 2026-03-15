from typing import Optional

from pydantic import BaseModel, Field

from ..auth import ROLE_VIEWER


class LoginInput(BaseModel):
    username: str
    password: str


class BootstrapAdminInput(BaseModel):
    username: str
    password: str
    display_name: str = ""


class UserUpsertInput(BaseModel):
    username: str
    password: Optional[str] = None
    role: str = Field(default=ROLE_VIEWER)
    display_name: str = ""
    disabled: bool = False
