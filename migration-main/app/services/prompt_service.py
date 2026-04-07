from pathlib import Path


PROMPTS_DIR = Path(__file__).resolve().parent.parent / "prompts"


def load_prompt_template(filename: str) -> str:
    prompt_path = PROMPTS_DIR / filename
    return prompt_path.read_text(encoding="utf-8").strip()


def render_prompt(filename: str, **kwargs) -> str:
    template = load_prompt_template(filename)
    return template.format(**kwargs)
