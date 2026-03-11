"use client";

type CollapsibleHeaderProps = {
  title: string;
  collapsed: boolean;
  onToggle: () => void;
  level?: 2 | 3;
};

export default function CollapsibleHeader({
  title,
  collapsed,
  onToggle,
  level = 2,
}: CollapsibleHeaderProps) {
  const Heading = level === 2 ? "h2" : "h3";

  return (
    <button type="button" className="collapsible-header" onClick={onToggle}>
      <span
        className={`collapsible-caret${collapsed ? " is-collapsed" : ""}`}
        aria-hidden="true"
      >
        ▾
      </span>
      <Heading>{title}</Heading>
    </button>
  );
}
