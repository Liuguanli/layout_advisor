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

    const updateActiveSection = () => {
      const activationLine = window.innerHeight * 0.22;
      const sections = items
        .map((item) => document.getElementById(item.id))
        .filter((element): element is HTMLElement => element !== null);

      const passedSections = sections.filter(
        (section) => section.getBoundingClientRect().top <= activationLine,
      );

      if (passedSections.length > 0) {
        const current = passedSections[passedSections.length - 1];
        setActiveSectionId(current.id);
        return;
      }

      const nearestSection = sections
        .map((section) => ({
          id: section.id,
          distance: Math.abs(section.getBoundingClientRect().top - activationLine),
        }))
        .sort((left, right) => left.distance - right.distance)[0];

      if (nearestSection) {
        setActiveSectionId(nearestSection.id);
      }
    };

    updateActiveSection();
    window.addEventListener("scroll", updateActiveSection, { passive: true });
    window.addEventListener("resize", updateActiveSection);

    return () => {
      window.removeEventListener("scroll", updateActiveSection);
      window.removeEventListener("resize", updateActiveSection);
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
