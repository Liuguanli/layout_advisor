"use client";

import { ReactNode } from "react";

type CollapsibleHeaderProps = {
  title: string;
  collapsed: boolean;
  onToggle: () => void;
  level?: 2 | 3;
  action?: ReactNode;
};

export default function CollapsibleHeader({
  title,
  collapsed,
  onToggle,
  level = 2,
  action,
}: CollapsibleHeaderProps) {
  const Heading = level === 2 ? "h2" : "h3";

  return (
    <div className="collapsible-header-row">
      <button type="button" className="collapsible-header" onClick={onToggle}>
        <span
          className={`collapsible-caret${collapsed ? " is-collapsed" : ""}`}
          aria-hidden="true"
        >
          ▾
        </span>
        <Heading>{title}</Heading>
      </button>
      {action ? <div className="collapsible-header-action">{action}</div> : null}
    </div>
  );
}
