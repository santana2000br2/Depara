"""One-shot: adiciona integrity/crossorigin nos CDNs versionados dos templates."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = ROOT / "templates"

REPLACEMENTS = [
    (
        '<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">',
        '<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" '
        'integrity="sha512-iecdLmaskl7CVkqkXNQ/ZH/XLlvWZOJyj7Yy7tcenmpD1ypASozpmT/E0iPtmFIB46ZmdtAc9eNBvH0H/ZpiBw==" '
        'crossorigin="anonymous" referrerpolicy="no-referrer">',
    ),
    (
        '<script src="https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js"></script>',
        '<script src="https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js" '
        'integrity="sha512-r22gChDnGvBylk90+2e/ycr3RVrDi8DIOkIGNhJlKfuyQM4tIRAI062MaV8sfjQKYVGjOBaZBOA87z+IhZE9DA==" '
        'crossorigin="anonymous" referrerpolicy="no-referrer"></script>',
    ),
]


def patch_bootstrap(text: str) -> str:
    css_needle = "https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css"
    js_needle = "https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/js/bootstrap.bundle.min.js"
    if css_needle in text and f'{css_needle}" integrity=' not in text:
        text = text.replace(
            f'{css_needle}"',
            f'{css_needle}" integrity="sha512-GQGU0fMMi238uA+a/bdWJfpUGKUkBdgfFdgBm72SUQ6BeyWjoY/ton0tEjH+OSH9iP4Dfh+7HM0I9f5eR0L/4w==" '
            'crossorigin="anonymous" referrerpolicy="no-referrer"',
        )
    if js_needle in text and f'{js_needle}" integrity=' not in text:
        text = text.replace(
            f'{js_needle}"',
            f'{js_needle}" integrity="sha512-pax4MlgXjHEPfCwcJLQhigY7+N8rt6bVvWLFyUMuxShv170X53TRzGPmPkZmGBhk+jikR8WBM4yl7A9WMHHqvg==" '
            'crossorigin="anonymous" referrerpolicy="no-referrer"',
        )
    return text


def main() -> None:
    changed = []
    files = list(TEMPLATES.rglob("*.html"))
    style_css = TEMPLATES / "style.css"
    if style_css.exists():
        files.append(style_css)

    for path in files:
        text = path.read_text(encoding="utf-8")
        original = text
        for old, new in REPLACEMENTS:
            text = text.replace(old, new)
        text = patch_bootstrap(text)
        if text != original:
            path.write_text(text, encoding="utf-8")
            changed.append(str(path.relative_to(ROOT)))

    print(f"changed={len(changed)}")
    for item in changed:
        print(item)


if __name__ == "__main__":
    main()
