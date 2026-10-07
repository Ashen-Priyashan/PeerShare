import { useEffect, useRef, useState } from "react";
import {
  ArchiveRegular,
  ArrowDownloadRegular,
  ArrowRightRegular,
  ArrowSyncRegular,
  CheckmarkRegular,
  ChevronRightRegular,
  DesktopRegular,
  DocumentPdfRegular,
  DocumentTableRegular,
  FolderOpenRegular,
  ImageRegular,
  InfoRegular,
  LaptopRegular,
  TextBulletListSquareRegular,
  VideoClipRegular,
  Wifi1Regular,
} from "@fluentui/react-icons";

const peers = [
  { id: "jamie", name: "Jamie’s Laptop", kind: "laptop", files: 6 },
  { id: "taylor", name: "Taylor’s PC", kind: "desktop", files: 8 },
  { id: "riley", name: "Riley’s Laptop", kind: "laptop", files: 4 },
  { id: "morgan", name: "Morgan’s PC", kind: "desktop", files: 11 },
];

const sharedFiles = [
  { id: "vacation", name: "Vacation Photos", size: "1.4 GB", type: "image" },
  { id: "trail-map", name: "Trail Map.pdf", size: "12 MB", type: "pdf" },
  { id: "birthday-video", name: "Birthday Video.mp4", size: "320 MB", type: "video" },
  { id: "dinner-ideas", name: "Dinner Ideas.txt", size: "4 KB", type: "text" },
  { id: "house-projects", name: "House Projects.xlsx", size: "28 MB", type: "sheet" },
  { id: "music-collection", name: "Music Collection.zip", size: "680 MB", type: "archive" },
];

const FileTypeIcon = ({ type }) => {
  const iconProps = { className: `file-type-icon file-type-${type}`, "aria-hidden": true };
  switch (type) {
    case "pdf":
      return <DocumentPdfRegular {...iconProps} />;
    case "video":
      return <VideoClipRegular {...iconProps} />;
    case "text":
      return <TextBulletListSquareRegular {...iconProps} />;
    case "sheet":
      return <DocumentTableRegular {...iconProps} />;
    case "archive":
      return <ArchiveRegular {...iconProps} />;
    default:
      return <ImageRegular {...iconProps} />;
  }
};

function PeerIcon({ kind, className = "" }) {
  const Icon = kind === "desktop" ? DesktopRegular : LaptopRegular;
  return <Icon className={className} aria-hidden="true" />;
}

function formatAddressError(value) {
  const address = value.trim();
  const match = /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})(?::(\d{1,5}))?$/.exec(address);
  if (!match || match.slice(1, 5).some((octet) => Number(octet) > 255)) {
    return "Enter an IPv4 address, for example 192.168.1.42.";
  }
  if (match[5] && (Number(match[5]) < 1 || Number(match[5]) > 65535)) {
    return "The port must be between 1 and 65535.";
  }
  return "";
}

export function App() {
  const [devices, setDevices] = useState(peers);
  const [selectedPeerId, setSelectedPeerId] = useState(peers[0].id);
  const [folderName, setFolderName] = useState("C:\\Users\\Alex\\PeerShare");
  const [folderFiles, setFolderFiles] = useState(12);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [address, setAddress] = useState("");
  const [addressError, setAddressError] = useState("");
  const [notice, setNotice] = useState("");
  const [activeDownload, setActiveDownload] = useState(null);
  const [completedDownloads, setCompletedDownloads] = useState(() => new Set());
  const folderInput = useRef(null);
  const selectedPeer = devices.find((peer) => peer.id === selectedPeerId) ?? devices[0];

  useEffect(() => {
    if (!activeDownload) return undefined;
    if (activeDownload.progress >= 100) {
      const finish = window.setTimeout(() => {
        setCompletedDownloads((current) => new Set(current).add(activeDownload.fileId));
        setNotice("Preview finished. Real file transfers will be connected in the next step.");
        setActiveDownload(null);
      }, 320);
      return () => window.clearTimeout(finish);
    }

    const tick = window.setTimeout(() => {
      setActiveDownload((current) => current && {
        ...current,
        progress: Math.min(100, current.progress + 12),
      });
    }, 100);
    return () => window.clearTimeout(tick);
  }, [activeDownload]);

  function handleFolderChange(event) {
    const files = Array.from(event.target.files ?? []);
    if (files.length === 0) return;
    const firstPath = files[0].webkitRelativePath || files[0].name;
    setFolderName(firstPath.includes("/") ? firstPath.split("/")[0] : firstPath);
    setFolderFiles(files.length);
    setNotice(`Selected a folder with ${files.length} files. Sharing is preview-only for now.`);
    event.target.value = "";
  }

  function handleRefresh() {
    if (isRefreshing) return;
    setIsRefreshing(true);
    setNotice("Searching the preview device list…");
    window.setTimeout(() => {
      setIsRefreshing(false);
      setNotice("Preview device search complete.");
    }, 1100);
  }

  function handleConnect(event) {
    event.preventDefault();
    const error = formatAddressError(address);
    setAddressError(error);
    if (error) return;

    const cleanAddress = address.trim();
    const existing = devices.find((peer) => peer.address === cleanAddress);
    if (existing) {
      setSelectedPeerId(existing.id);
    } else {
      const addedPeer = {
        id: `manual-${Date.now()}`,
        address: cleanAddress,
        name: cleanAddress,
        kind: "desktop",
        files: sharedFiles.length,
      };
      setDevices((current) => [...current, addedPeer]);
      setSelectedPeerId(addedPeer.id);
    }
    setAddress("");
    setNotice("Added to the preview list. A real peer connection is not active yet.");
  }

  function handleDownload(file) {
    if (activeDownload || completedDownloads.has(file.id)) return;
    setActiveDownload({ fileId: file.id, progress: 0 });
  }

  return (
    <div className="desktop-app">
      <header className="titlebar">
        <img className="brand-mark" src="/assets/peershare-mark.png" alt="" />
        <span className="brand-name">PeerShare</span>
      </header>

      <main className="workspace">
        <section className="sharing-column" aria-label="Share and find devices">
          <div className="welcome-copy">
            <Wifi1Regular className="welcome-icon" aria-hidden="true" />
            <h1>Share files with<br className="wide-break" /> people nearby.</h1>
            <p>Find nearby devices, browse their shared files,<br className="wide-break" /> and download what you need — all on your local network.</p>
          </div>

          <section className="shared-folder" aria-labelledby="shared-folder-title">
            <div className="folder-icon-wrap"><FolderOpenRegular aria-hidden="true" /></div>
            <div className="folder-details">
              <h2 id="shared-folder-title">Your shared folder</h2>
              <p className="folder-path" title={folderName}>{folderName}</p>
              <p className="folder-hint">{folderFiles} files · preview selection only</p>
            </div>
            <button className="change-folder" type="button" onClick={() => folderInput.current?.click()}>Change</button>
            <input
              ref={folderInput}
              className="visually-hidden"
              type="file"
              multiple
              webkitdirectory=""
              directory=""
              onChange={handleFolderChange}
              aria-label="Choose a shared folder"
            />
          </section>

          <section className="nearby-section" aria-labelledby="nearby-title">
            <div className="section-heading">
              <h2 id="nearby-title">Nearby devices</h2>
              <button className="refresh-button" type="button" onClick={handleRefresh} disabled={isRefreshing}>
                <ArrowSyncRegular className={isRefreshing ? "spin" : ""} aria-hidden="true" />
                <span>{isRefreshing ? "Looking for devices…" : "Refresh"}</span>
              </button>
            </div>
            <div className="peer-list" role="listbox" aria-label="Nearby devices">
              {devices.map((device) => (
                <button
                  key={device.id}
                  className={`peer-row${device.id === selectedPeerId ? " selected" : ""}`}
                  type="button"
                  role="option"
                  aria-selected={device.id === selectedPeerId}
                  onClick={() => setSelectedPeerId(device.id)}
                >
                  <span className="device-icon-wrap"><PeerIcon kind={device.kind} className="device-icon" /></span>
                  <span className="peer-copy">
                    <span className="peer-name">{device.name}</span>
                    <span className="peer-state">Available</span>
                  </span>
                  <ChevronRightRegular className="peer-chevron" aria-hidden="true" />
                </button>
              ))}
            </div>
          </section>

          <form className="connect-form" onSubmit={handleConnect}>
            <label htmlFor="peer-address">Connect to a device by IP address</label>
            <div className="connect-row">
              <input
                id="peer-address"
                type="text"
                inputMode="decimal"
                autoComplete="off"
                placeholder="e.g. 192.168.1.42"
                value={address}
                aria-invalid={Boolean(addressError)}
                aria-describedby={addressError ? "address-error" : undefined}
                onChange={(event) => {
                  setAddress(event.target.value);
                  setAddressError("");
                }}
              />
              <button className="connect-button" type="submit" aria-label="Connect to device">
                <span>Connect</span>
                <ArrowRightRegular aria-hidden="true" />
              </button>
            </div>
            {addressError && <p id="address-error" className="field-error" role="alert">{addressError}</p>}
          </form>

          {notice && <p className="notice" role="status"><InfoRegular aria-hidden="true" />{notice}</p>}
        </section>

        <section className="remote-panel" aria-labelledby="selected-peer-name">
          {selectedPeer ? (
            <>
              <header className="remote-header">
                <div className="selected-device-icon"><PeerIcon kind={selectedPeer.kind} /></div>
                <div className="selected-peer-copy">
                  <h2 id="selected-peer-name">{selectedPeer.name}</h2>
                  <p><span className="available-dot" />Available <span className="header-separator">·</span> {selectedPeer.files} files</p>
                </div>
              </header>

              <div className="files-heading">
                <h3>Shared files</h3>
                <span>{selectedPeer.files} files</span>
              </div>

              <div className="file-list" role="list" aria-label={`Files shared by ${selectedPeer.name}`}>
                {sharedFiles.map((file) => {
                  const active = activeDownload?.fileId === file.id;
                  const completed = completedDownloads.has(file.id);
                  return (
                    <div className={`file-row${active ? " downloading" : ""}`} role="listitem" key={file.id}>
                      <span className={`file-icon-wrap${file.type === "image" ? " image-preview" : ""}`}>
                        {file.type === "image" ? <img src="/assets/vacation-lake.png" alt="" /> : <FileTypeIcon type={file.type} />}
                      </span>
                      <span className="file-copy">
                        <span className="file-name">{file.name}</span>
                        <span className="file-size">{file.size}</span>
                        {active && <span className="progress-track"><span style={{ width: `${activeDownload.progress}%` }} /></span>}
                      </span>
                      <button
                        className={`download-button${completed ? " complete" : ""}`}
                        type="button"
                        onClick={() => handleDownload(file)}
                        disabled={Boolean(activeDownload && !active) || completed}
                        aria-label={`${completed ? "Downloaded" : active ? `Downloading ${activeDownload.progress}%` : "Download"} ${file.name}`}
                      >
                        {completed ? <CheckmarkRegular aria-hidden="true" /> : <ArrowDownloadRegular aria-hidden="true" />}
                        <span>{completed ? "Saved" : active ? `${activeDownload.progress}%` : "Download"}</span>
                      </button>
                    </div>
                  );
                })}
              </div>
            </>
          ) : (
            <div className="empty-state">
              <PeerIcon kind="desktop" />
              <h2>No device selected</h2>
              <p>Choose a nearby device to see its shared files.</p>
            </div>
          )}
        </section>
      </main>
      <div className="sr-only" aria-live="polite">Preview data only; networking is not connected.</div>
    </div>
  );
}
