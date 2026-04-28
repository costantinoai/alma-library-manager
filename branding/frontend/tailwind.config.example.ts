import type { Config } from "tailwindcss";

const config: Config = {
  theme: {
    extend: {
      colors: {
        alma: {
          navy: "#0F1E36",
          ink: "#09162A",
          teal: "#0E6F6D",
          "pale-blue": "#B7D2E4",
          parchment: "#F3EAD6",
          gold: "#C49A45",
          paper: "#FFFCF7",
          cream: "#FFF9F0",
        },
      },
      fontFamily: {
        brand: ["Merriweather", "Georgia", "Times New Roman", "serif"],
        ui: ["Inter", "ui-sans-serif", "system-ui", "sans-serif"],
      },
      borderRadius: {
        app: "24%",
      },
      boxShadow: {
        alma: "0 12px 32px rgb(9 22 42 / .12)",
        "alma-lg": "0 24px 60px rgb(9 22 42 / .18)",
      },
    },
  },
};

export default config;
