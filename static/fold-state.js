const FOLD_STATE_STORAGE_KEY = "train_watch_fold_state_v1";

function readFoldState() {
  try {
    const raw = localStorage.getItem(FOLD_STATE_STORAGE_KEY);
    if (!raw) return {};
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch (_error) {
    return {};
  }
}

function writeFoldState(state) {
  try {
    localStorage.setItem(FOLD_STATE_STORAGE_KEY, JSON.stringify(state));
  } catch (_error) {
    // Ignore storage failures and fall back to DOM defaults.
  }
}

function setFoldState(foldId, open) {
  if (!foldId) return;
  const nextState = readFoldState();
  nextState[foldId] = Boolean(open);
  writeFoldState(nextState);
}

export function applyFoldState(root = document) {
  const foldState = readFoldState();
  root.querySelectorAll("details[data-fold-id]").forEach((element) => {
    const foldId = element.dataset.foldId || "";
    if (Object.prototype.hasOwnProperty.call(foldState, foldId)) {
      element.open = Boolean(foldState[foldId]);
    }
    if (element.dataset.foldBound === "1") return;
    element.dataset.foldBound = "1";
    element.addEventListener("toggle", () => {
      setFoldState(foldId, element.open);
    });
  });
}
