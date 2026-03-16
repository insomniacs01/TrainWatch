import re
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional

NUMBER = r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?"


@dataclass
class ParsedTrainingState:
    parser: str
    epoch: Optional[int] = None
    step: Optional[int] = None
    step_total: Optional[int] = None
    loss: Optional[float] = None
    eval_loss: Optional[float] = None
    lr: Optional[float] = None
    grad_norm: Optional[float] = None
    tokens_per_sec: Optional[float] = None
    samples_per_sec: Optional[float] = None
    eta: str = ""
    eta_seconds: Optional[int] = None
    last_log_line: str = ""
    completion_matched: bool = False
    error_matched: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _extract_float(pattern: str, text: str) -> Optional[float]:
    match = re.search(pattern, text, flags=re.IGNORECASE)
    if not match:
        return None
    try:
        return float(match.group(1))
    except (TypeError, ValueError):
        return None


def _extract_int(pattern: str, text: str) -> Optional[int]:
    match = re.search(pattern, text, flags=re.IGNORECASE)
    if not match:
        return None
    try:
        return int(match.group(1))
    except (TypeError, ValueError):
        return None


def _parse_eta_seconds(eta: str) -> Optional[int]:
    if not eta or ":" not in eta:
        return None
    parts = [part.strip() for part in eta.split(":") if part.strip()]
    try:
        ints = [int(part) for part in parts]
    except ValueError:
        return None
    if len(ints) == 3:
        return ints[0] * 3600 + ints[1] * 60 + ints[2]
    if len(ints) == 2:
        return ints[0] * 60 + ints[1]
    return None


def _last_non_empty_line(lines: List[str]) -> str:
    for line in reversed(lines):
        line = line.strip()
        if line:
            return line
    return ""


def _extract_epoch_step(line: str) -> Dict[str, Optional[int]]:
    return {
        "epoch": _extract_int(r"(?:Train\s+)?Epoch:\s*\[(\d+)\]", line),
        "step": _extract_int(r"\[(\d+)\s*/\s*\d+\]", line),
        "step_total": _extract_int(r"\[\d+\s*/\s*(\d+)\]", line),
    }


def _parse_common_metrics(line: str) -> Dict[str, Optional[float]]:
    return {
        "loss": _extract_float(r"(?:^|\s)loss[:=\s]+(" + NUMBER + r")", line),
        "eval_loss": _extract_float(r"(?:eval[_\s-]*loss|val[_\s-]*loss)[:=\s]+(" + NUMBER + r")", line),
        "lr": _extract_float(r"(?:^|\s)lr[:=\s]+(" + NUMBER + r")", line),
        "grad_norm": _extract_float(r"grad[_\s-]*norm[:=\s]+(" + NUMBER + r")", line),
        "tokens_per_sec": _extract_float(
            r"(?:tokens?/sec|tokens?/s|tok/sec|tok/s|toks?/s)[:=\s]+(" + NUMBER + r")",
            line,
        ),
        "samples_per_sec": _extract_float(
            r"(?:samples?/sec|samples?/s|imgs?/sec|imgs?/s)[:=\s]+(" + NUMBER + r")",
            line,
        ),
    }


def _collect_signal_lines(text: str) -> Dict[str, str]:
    lines = [line.rstrip() for line in text.splitlines() if line.strip()]
    last_train = ""
    last_eval = ""
    last_any = _last_non_empty_line(lines)
    for line in reversed(lines):
        if not last_eval and re.search(r"Test\s+Epoch|eval|validation", line, flags=re.IGNORECASE):
            last_eval = line
        is_eval_line = bool(re.search(r"Test\s+Epoch|eval|validation", line, flags=re.IGNORECASE))
        if (
            not last_train
            and not is_eval_line
            and re.search(r"Epoch:|step=|global_step|loss", line, flags=re.IGNORECASE)
        ):
            last_train = line
        if last_train and last_eval:
            break
    return {"last_any": last_any, "last_train": last_train or last_any, "last_eval": last_eval}


def parse_mapanything(text: str) -> ParsedTrainingState:
    signals = _collect_signal_lines(text)
    train_line = signals["last_train"]
    eval_line = signals["last_eval"]
    state = ParsedTrainingState(parser="mapanything", last_log_line=signals["last_any"])
    state.eta = (
        re.search(r"eta:\s*([0-9:]+)", train_line, flags=re.IGNORECASE).group(1)
        if re.search(r"eta:\s*([0-9:]+)", train_line, flags=re.IGNORECASE)
        else ""
    )
    state.eta_seconds = _parse_eta_seconds(state.eta)
    epoch_step = _extract_epoch_step(train_line)
    state.epoch = epoch_step["epoch"]
    state.step = epoch_step["step"]
    state.step_total = epoch_step["step_total"]
    metrics = _parse_common_metrics(train_line)
    state.loss = metrics["loss"]
    state.lr = metrics["lr"]
    state.grad_norm = metrics["grad_norm"]
    if eval_line:
        eval_metrics = _parse_common_metrics(eval_line)
        state.eval_loss = eval_metrics["loss"] or eval_metrics["eval_loss"]
        if state.loss is None and state.eval_loss is not None:
            state.loss = state.eval_loss
    return state


def parse_generic_torch(text: str) -> ParsedTrainingState:
    signals = _collect_signal_lines(text)
    line = signals["last_train"]
    state = ParsedTrainingState(parser="generic_torch", last_log_line=signals["last_any"])
    state.eta = (
        re.search(r"eta[:=\s]+([0-9:]+)", line, flags=re.IGNORECASE).group(1)
        if re.search(r"eta[:=\s]+([0-9:]+)", line, flags=re.IGNORECASE)
        else ""
    )
    state.eta_seconds = _parse_eta_seconds(state.eta)
    state.epoch = _extract_int(r"epoch[:=\s\[]+(\d+)", line)
    state.step = _extract_int(r"(?:step|iter|global_step)[:=\s]+(\d+)", line) or _extract_epoch_step(line)["step"]
    state.step_total = (
        _extract_int(r"(?:step_total|total_steps|iters?)[:=\s]+(\d+)", line) or _extract_epoch_step(line)["step_total"]
    )
    metrics = _parse_common_metrics(line)
    state.loss = metrics["loss"]
    state.eval_loss = metrics["eval_loss"]
    state.lr = metrics["lr"]
    state.grad_norm = metrics["grad_norm"]
    state.tokens_per_sec = metrics["tokens_per_sec"]
    state.samples_per_sec = metrics["samples_per_sec"]
    return state


def parse_deepspeed(text: str) -> ParsedTrainingState:
    signals = _collect_signal_lines(text)
    line = signals["last_train"]
    state = ParsedTrainingState(parser="deepspeed", last_log_line=signals["last_any"])
    state.epoch = _extract_int(r"epoch[:=\s\[]+(\d+)", line)
    state.step = _extract_int(r"(?:step|global_step)[:=\s]+(\d+)", line)
    state.step_total = _extract_int(r"(?:total_steps|steps_total)[:=\s]+(\d+)", line)
    metrics = _parse_common_metrics(line)
    state.loss = metrics["loss"]
    state.eval_loss = metrics["eval_loss"]
    state.lr = metrics["lr"]
    state.grad_norm = metrics["grad_norm"]
    state.tokens_per_sec = metrics["tokens_per_sec"]
    state.samples_per_sec = metrics["samples_per_sec"]
    eta_match = re.search(r"eta[:=\s]+([0-9:]+)", line, flags=re.IGNORECASE)
    if eta_match:
        state.eta = eta_match.group(1)
        state.eta_seconds = _parse_eta_seconds(state.eta)
    return state


def parse_training_output(
    parser_name: str,
    text: str,
    completion_regex: str,
    error_regex: str,
) -> ParsedTrainingState:
    parser_key = (parser_name or "auto").strip().lower()
    if parser_key in ("auto", "mapanything"):
        state = parse_mapanything(text)
        if state.loss is None and parser_key == "auto":
            generic = parse_generic_torch(text)
            if generic.loss is not None or generic.step is not None:
                state = generic
        if state.loss is None and parser_key == "auto":
            deepspeed = parse_deepspeed(text)
            if deepspeed.loss is not None or deepspeed.step is not None:
                state = deepspeed
    elif parser_key == "generic_torch":
        state = parse_generic_torch(text)
    elif parser_key == "deepspeed":
        state = parse_deepspeed(text)
    else:
        state = parse_generic_torch(text)
        state.parser = parser_key

    state.completion_matched = bool(completion_regex and re.search(completion_regex, text, flags=re.IGNORECASE))
    state.error_matched = bool(error_regex and re.search(error_regex, text, flags=re.IGNORECASE))
    return state
