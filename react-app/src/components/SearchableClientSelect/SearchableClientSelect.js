import React, { useState, useMemo, useRef, useEffect } from "react";
import "./SearchableClientSelect.css";

function SearchableClientSelect({ label, options = [], onSelect }) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const containerRef = useRef(null);

  useEffect(() => {
    const handleClickOutside = (e) => {
      if (containerRef.current && !containerRef.current.contains(e.target)) {
        setOpen(false);
      }
    };
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  const filteredOptions = useMemo(() => {
    if (!query) return options;
    return options.filter((opt) =>
      opt.label.toLowerCase().includes(query.toLowerCase())
    );
  }, [query, options]);

  const handleSelect = (opt) => {
    setQuery(opt.label);
    setOpen(false);
    onSelect(opt.value);
  };

  const clearInput = () => {
    setQuery("");
    setOpen(false);
  };

  return (
    <div className="scs-wrapper" ref={containerRef}>
      <label className="scs-label">{label}</label>

      <div
        className={`scs-input ${open ? "active" : ""}`}
        onClick={() => setOpen(true)}
      >
        <span className="scs-icon">🔍</span>

        <input
          value={query}
          placeholder="Search Account Code..."
          onChange={(e) => {
            setQuery(e.target.value);
            setOpen(true);
          }}
        />

        {query && (
          <span className="scs-clear" onClick={clearInput}>
            ✕
          </span>
        )}

        <span className="scs-arrow">▾</span>
      </div>

      {open && filteredOptions.length > 0 && (
        <div className="scs-dropdown">
          <div className="scs-search-hint">Search Account Code...</div>

          <div className="scs-options">
            {filteredOptions.map((opt) => (
              <div
                key={opt.value}
                className="scs-option"
                onClick={() => handleSelect(opt)}
              >
                {opt.label}
              </div>
            ))}
          </div>

          <div className="scs-footer">
            {filteredOptions.length} of {options.length} options
          </div>
        </div>
      )}
    </div>
  );
}

export default SearchableClientSelect;
