"use client";

import { useEffect, useState } from "react";

type NavItem = {
  id: string;
  label: string;
  note: string;
};

type RightSidebarNavProps = {
  items: NavItem[];
};

export default function RightSidebarNav({ items }: RightSidebarNavProps) {
  const [activeSectionId, setActiveSectionId] = useState<string>(items[0]?.id ?? "");

  useEffect(() => {
    if (items.length === 0) {
      return undefined;
    }

    const observer = new IntersectionObserver(
      (entries) => {
        const visibleEntries = entries
          .filter((entry) => entry.isIntersecting)
          .sort((left, right) => right.intersectionRatio - left.intersectionRatio);
        if (visibleEntries.length > 0) {
          setActiveSectionId(visibleEntries[0].target.id);
        }
      },
      {
        rootMargin: "-15% 0px -55% 0px",
        threshold: [0.1, 0.25, 0.45, 0.7],
      },
    );

    items.forEach((item) => {
      const element = document.getElementById(item.id);
      if (element) {
        observer.observe(element);
      }
    });

    return () => {
      observer.disconnect();
    };
  }, [items]);

  return (
    <aside className="side-nav">
      <div className="side-nav-card">
        <p className="side-nav-eyebrow">Sections</p>
        <nav>
          <ul className="side-nav-list">
            {items.map((item, index) => (
              <li key={item.id}>
                <a
                  href={`#${item.id}`}
                  className={`side-nav-link ${activeSectionId === item.id ? "is-active" : ""}`}
                >
                  <span className="side-nav-index">{String(index + 1).padStart(2, "0")}</span>
                  <span>
                    <strong>{item.label}</strong>
                    <small>{item.note}</small>
                  </span>
                </a>
              </li>
            ))}
          </ul>
        </nav>
      </div>
    </aside>
  );
}
