# ALMa favicon assets

Use `favicon.svg` as the canonical favicon source.

Recommended HTML:

```html
<link rel="icon" href="/favicon.svg" type="image/svg+xml" />
<link rel="mask-icon" href="/mask-icon.svg" color="#0F1E36" />
<link rel="manifest" href="/manifest.webmanifest" />
<meta name="theme-color" content="#0F1E36" />
```

For production, generate PNG fallbacks from the SVG at 192×192, 512×512, and Apple touch icon sizes if your build pipeline requires them.
