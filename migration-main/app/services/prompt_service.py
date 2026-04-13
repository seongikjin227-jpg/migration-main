"""프롬프트 템플릿 로더/렌더러."""

from pathlib import Path


PROMPTS_DIR = Path(__file__).resolve().parent.parent / "prompts"


def load_prompt_template(filename: str) -> str:
    """`app/prompts`에서 템플릿 파일 1개를 읽는다."""
    prompt_path = PROMPTS_DIR / filename
    return prompt_path.read_text(encoding="utf-8").strip()


def render_prompt(filename: str, **kwargs) -> str:
    """템플릿 placeholder를 키워드 인자로 치환한다."""
    template = load_prompt_template(filename)
    return template.format(**kwargs)
