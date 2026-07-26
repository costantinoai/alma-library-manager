# ALMa logo usage

## Primary rule

Use `alma-logo-horizontal.svg` as the primary product logo in web headers, docs, and landing pages.

## Name

The product name is **ALMa**.

Do not use:

- ALMA Shelf
- Alma Shelf
- ALMA Library
- Alma

## Clear space

Keep at least the width of the teal bookmark around the mark on all sides.

## Minimum sizes

- Full horizontal logo: 180px wide minimum.
- Stacked logo: 160px wide minimum.
- Mark only: 32px wide minimum.
- App icon: 48px wide minimum.

## Backgrounds

- Use the full-color logo on white, warm paper, or very light pale blue.
- Use the app icon on complex or dark surfaces.
- Use `alma-mark-monochrome.svg` when the mark must be single color.

## Avoid

- Do not recolor individual books independently.
- Do not remove the teal bookmark from the central A-book mark.
- Do not stretch or compress the mark.
- Do not apply extra shadows beyond the provided icon treatment.

## Profile picture / avatar (Slack, GitHub, social)

**Files:** `logo/alma-avatar-square.svg` (master) · `logo/alma-avatar-1024.png`
(1024×1024, ready to upload).

Built from the **stacked** logo on a square `paper` (`#FFFCF7`) field, with
~15% padding on every side because Slack rounds avatar corners and other
platforms crop to a circle. 1024px sits comfortably inside Slack's 512–2000px
requirement.

Regenerate the PNG from the master (never edit the PNG by hand):

```bash
inkscape branding/logo/alma-avatar-square.svg --export-type=png \
  --export-filename=branding/logo/alma-avatar-1024.png -w 1024 -h 1024
convert branding/logo/alma-avatar-1024.png -background '#FFFCF7' \
  -alpha remove -alpha off branding/logo/alma-avatar-1024.png
```

Two things the master deliberately changes from `alma-logo-stacked.svg`:

* **No `feDropShadow`.** Several rasterizers (Inkscape included) don't support
  it and silently drop the *entire filtered group* — the first export of this
  avatar came out as a bare wordmark with the bookshelf missing. Flat art
  renders everywhere.
* **No white backdrop rect.** The stacked logo paints its own `#fff` panel,
  which showed as a visible white square inset on the paper field.

At small sizes the wordmark and tagline stop being legible. Where the avatar
renders below ~64px, prefer `logo/alma-app-icon.svg` — the mark alone, designed
for exactly that.
