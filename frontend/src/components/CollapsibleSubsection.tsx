"use client";

import { ReactNode, useState } from "react";

import CollapsibleHeader from "./CollapsibleHeader";

type CollapsibleSubsectionProps = {
  title: string;
  children: ReactNode;
  note?: ReactNode;
  defaultCollapsed?: boolean;
  actions?: ReactNode;
  className?: string;
};

export default function CollapsibleSubsection({
  title,
  children,
  note,
  defaultCollapsed = false,
  actions,
  className = "summary-block",
}: CollapsibleSubsectionProps) {
  const [collapsed, setCollapsed] = useState(defaultCollapsed);
  const containerClassName = `subsection-block ${className}`;

  return (
    <div className={containerClassName}>
      <div className="subsection-toggle-row">
        <CollapsibleHeader
          title={title}
          collapsed={collapsed}
          onToggle={() => setCollapsed((current) => !current)}
          level={3}
        />
        {actions && <div className="subsection-actions">{actions}</div>}
      </div>
      {!collapsed && (
        <div className="subsection-content">
          {note && <div className="subsection-note">{note}</div>}
          {children}
        </div>
      )}
    </div>
  );
}
