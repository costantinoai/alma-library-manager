import * as React from "react";

export type LogoMarkProps = Omit<React.ImgHTMLAttributes<HTMLImageElement>, "src" | "alt"> & {
  src?: string;
  alt?: string;
};

export function LogoMark({
  src = "/branding/logo/alma-mark-source.svg",
  alt = "ALMa logo mark",
  ...props
}: LogoMarkProps) {
  return <img src={src} alt={alt} {...props} />;
}

export default LogoMark;
