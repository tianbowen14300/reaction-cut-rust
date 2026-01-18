import { useEffect, useMemo, useState } from "react";
import AnchorSection from "./sections/AnchorSection";
import DownloadSection from "./sections/DownloadSection";
import SubmissionSection from "./sections/SubmissionSection";
import SettingsSection from "./sections/SettingsSection";
import BiliToolsLoginSection from "./sections/BiliToolsLoginSection";
import ToolboxSection from "./sections/ToolboxSection";
import { invokeCommand } from "./lib/tauri";

const sections = [
  { id: "anchor", label: "主播订阅", short: "订" },
  { id: "download", label: "视频下载", short: "下" },
  { id: "submission", label: "视频投稿", short: "投" },
  { id: "toolbox", label: "工具箱", short: "工" },
  { id: "settings", label: "设置", short: "设" },
];

const sectionLabels = {
  auth: "登录",
  anchor: "主播订阅",
  download: "视频下载",
  submission: "视频投稿",
  toolbox: "工具箱",
  settings: "设置",
};

function App() {
  const [active, setActive] = useState("download");
  const [authStatus, setAuthStatus] = useState({ loggedIn: false });
  const [avatarPreview, setAvatarPreview] = useState("");

  const activeLabel = useMemo(() => {
    return sectionLabels[active] || "";
  }, [active]);

  const avatarUrl = useMemo(() => {
    const raw = authStatus?.userInfo || {};
    const level1 = raw?.data || raw;
    const level2 = level1?.data || level1;
    return (
      level2?.avatar ||
      level2?.face ||
      level1?.avatar ||
      level1?.face ||
      ""
    );
  }, [authStatus]);

  const refreshAuthStatus = async () => {
    try {
      const data = await invokeCommand("auth_status");
      setAuthStatus(data || { loggedIn: false });
    } catch (error) {
      setAuthStatus((prev) => prev || { loggedIn: false });
    }
  };

  useEffect(() => {
    refreshAuthStatus();
  }, []);

  useEffect(() => {
    const loadAvatar = async () => {
      if (!authStatus?.loggedIn || !avatarUrl) {
        setAvatarPreview("");
        return;
      }
      try {
        const data = await invokeCommand("video_proxy_image", { url: avatarUrl });
        setAvatarPreview(data || "");
        await invokeCommand("auth_client_log", {
          message: `app_avatar_proxy_ok:${String(avatarUrl).length}:${String(data || "").length}`,
        });
      } catch (error) {
        const message = error?.message || String(error || "");
        await invokeCommand("auth_client_log", {
          message: `app_avatar_proxy_fail:${String(avatarUrl).length}:${message}`,
        });
      }
    };
    loadAvatar();
  }, [authStatus?.loggedIn, avatarUrl]);

  const renderSection = () => {
    switch (active) {
      case "auth":
        return <BiliToolsLoginSection onStatusChange={setAuthStatus} initialStatus={authStatus} />;
      case "anchor":
        return <AnchorSection />;
      case "download":
        return <DownloadSection />;
      case "submission":
        return <SubmissionSection />;
      case "toolbox":
        return <ToolboxSection />;
      case "settings":
        return <SettingsSection />;
      default:
        return null;
    }
  };

  return (
    <div className="app-shell">
      <aside className="sidebar">
        {sections.map((item) => (
          <button
            key={item.id}
            className={active === item.id ? "active" : ""}
            onClick={() => setActive(item.id)}
            title={item.label}
          >
            <span className="menu-label">{item.label}</span>
          </button>
        ))}
      </aside>
      <div id="main" className="main-shell">
        <div className="title-bar" data-tauri-drag-region>
          <div className="flex items-center gap-3 text-[var(--content-color)]">
            <span className="text-sm font-semibold">Reaction Cut</span>
            <span className="text-xs text-[var(--desc-color)]">{activeLabel}</span>
          </div>
          <button
            className={`avatar-btn ${active === "auth" ? "active" : ""}`}
            onClick={() => setActive("auth")}
            title="登录"
            data-tauri-drag-region="false"
          >
            {authStatus?.loggedIn && avatarPreview ? (
              <img
                src={avatarPreview}
                alt="用户头像"
                onError={() => setAvatarPreview("")}
              />
            ) : (
              <span className="avatar-fallback" />
            )}
          </button>
        </div>
        <div className="content-wrap">
          <div className="page">
            <div className="page-scroll">{renderSection()}</div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default App;
