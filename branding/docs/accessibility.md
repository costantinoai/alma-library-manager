# ALMa frontend accessibility notes

## Color contrast

Safe text/background pairings:

- Navy `#0F1E36` on Paper `#FFFCF7`
- Ink `#09162A` on Cream `#FFF9F0`
- Navy `#0F1E36` on Pale Blue `#B7D2E4`
- Cream `#FFF9F0` on Navy `#0F1E36`

Avoid using Gold `#C49A45` for small text. Use it for separators and decorative trim only.

## Logo alt text

Use one of these:

- `ALMa — Another Library Manager`
- `ALMa logo`
- Empty `alt=""` only when the word ALMa appears adjacent in visible text.

## Focus states

Use the `alma-focus-ring` utility from `tokens.css` for interactive controls.

## Motion

For future loading animations, prefer a subtle bookmark rise or page reveal. Respect `prefers-reduced-motion`.
