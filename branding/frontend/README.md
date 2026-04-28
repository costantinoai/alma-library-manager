# ALMa frontend kit

This kit is tied to the corrected **ALMa** brand system: no “Shelf” naming, and assets preserve the bookshelf/A-book identity from the selected option 5 direction.

## Contents

- `tokens.css` — CSS custom properties
- `tokens.ts` / `tokens.json` — JS/TS design tokens
- `tailwind.config.example.ts` — Tailwind extension
- `components/` — React components backed by the canonical SVG assets
- `favicon/` — SVG favicon and web manifest
- `ui-examples/` — plain HTML implementation examples
- `docs/` — usage and accessibility notes

## React example

```tsx
import { Logo, AppIcon } from "./components";

export function Header() {
  return (
    <header>
      <Logo width={220} />
      <AppIcon width={48} height={48} />
    </header>
  );
}
```

## CSS example

```css
@import "./tokens.css";

body {
  background: var(--color-bg);
  color: var(--color-text);
  font-family: var(--font-ui);
}
```
