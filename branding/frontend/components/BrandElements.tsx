import * as React from "react";

export type BrandElementsProps = Omit<React.ImgHTMLAttributes<HTMLImageElement>, "src" | "alt"> & {
  src?: string;
  alt?: string;
};

export function BrandElements({
  src = "/branding/logo/alma-brand-elements.svg",
  alt = "ALMa brand elements: bookmark, paper, library, discover",
  ...props
}: BrandElementsProps) {
  return <img src={src} alt={alt} {...props} />;
}

export default BrandElements;
