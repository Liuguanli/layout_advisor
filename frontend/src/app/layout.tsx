import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "LayoutPilot: A Lakehouse Physical Design Advisor",
  description: "Interactive prototype for LayoutPilot: A Lakehouse Physical Design Advisor",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
