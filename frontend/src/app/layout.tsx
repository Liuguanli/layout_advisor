import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Layout Exploration Prototype",
  description: "Dataset and SQL workload analysis prototype",
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
