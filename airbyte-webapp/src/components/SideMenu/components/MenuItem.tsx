import classnames from "classnames";
import React from "react";

import styles from "./MenuItem.module.scss";

interface MenuItemProps {
  name: string | React.ReactNode;
  isActive?: boolean;
  count?: number;
  id?: string;
  onClick: () => void;
}

const MenuItem: React.FC<MenuItemProps> = ({ count, isActive, name, id, onClick }) => {
  const menuItemStyle = classnames(styles.menuItem, {
    [styles.active]: isActive,
  });

  return (
    <div data-testid={id} onClick={onClick} className={menuItemStyle}>
      {name}
      {count ? <div className={styles.counter}>{count}</div> : null}
    </div>
  );
};

export default MenuItem;
