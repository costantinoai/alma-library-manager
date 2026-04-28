import * as React from "react";

export type LogoProps = Omit<React.ImgHTMLAttributes<HTMLImageElement>, "src" | "alt"> & {
  variant?: "horizontal" | "stacked" | "wordmark";
  src?: string;
  alt?: string;
};

const sources = {
  horizontal: "/branding/logo/alma-logo-horizontal.svg",
  stacked: "/branding/logo/alma-logo-stacked.svg",
  wordmark: "/branding/logo/alma-wordmark.svg",
};

export function Logo({
  variant = "horizontal",
  src,
  alt = "ALMa — Another Library Manager",
  ...props
}: LogoProps) {
  return <img src={src ?? sources[variant]} alt={alt} {...props} />;
}

export default Logo;
