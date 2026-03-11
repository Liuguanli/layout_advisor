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

  return (
    <div className={className}>
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
        <>
          {note && <div className="subsection-note">{note}</div>}
          {children}
        </>
      )}
    </div>
  );
}
