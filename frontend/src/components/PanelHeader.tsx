"use client";

import { ReactNode } from "react";

type PanelHeaderProps = {
  title: string;
  action?: ReactNode;
  level?: 2 | 3;
};

export default function PanelHeader({
  title,
  action,
  level = 2,
}: PanelHeaderProps) {
  const Heading = level === 2 ? "h2" : "h3";

  return (
    <div className="panel-header">
      <Heading>{title}</Heading>
      {action ? <div className="collapsible-header-action">{action}</div> : null}
    </div>
  );
}
