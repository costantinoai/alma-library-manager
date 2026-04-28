import * as React from "react";

export type AppIconProps = Omit<React.ImgHTMLAttributes<HTMLImageElement>, "src" | "alt"> & {
  src?: string;
  alt?: string;
};

export function AppIcon({
  src = "/branding/logo/alma-app-icon.svg",
  alt = "ALMa app icon",
  ...props
}: AppIconProps) {
  return <img src={src} alt={alt} {...props} />;
}

export default AppIcon;
