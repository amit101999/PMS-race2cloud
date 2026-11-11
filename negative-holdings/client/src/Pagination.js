// Copy of the Pagination component in react-app/src/components/common/CommonComponents.js.

const containerStyle = {
  display: "flex",
  alignItems: "center",
  justifyContent: "space-between",
  padding: "12px 16px",
  fontSize: 12,
  color: "#4b5563",
};

const controlsStyle = {
  display: "flex",
  alignItems: "center",
  gap: 8,
};

const buttonStyle = (disabled) => ({
  padding: "4px 8px",
  borderRadius: 4,
  border: "1px solid #e5e7eb",
  backgroundColor: disabled ? "#f9fafb" : "#ffffff",
  color: disabled ? "#9ca3af" : "#374151",
  fontSize: 11,
  cursor: disabled ? "default" : "pointer",
});

function Pagination({ currentPage, pageSize, totalRows, onPageChange }) {
  const totalPages = Math.max(1, Math.ceil(totalRows / pageSize || 1));
  const disabledPrev = currentPage <= 1;
  const disabledNext = currentPage >= totalPages;

  return (
    <div style={containerStyle}>
      <div style={controlsStyle}>
        <button
          type="button"
          style={buttonStyle(disabledPrev)}
          disabled={disabledPrev}
          onClick={() => onPageChange(1)}
        >
          « First
        </button>
        <button
          type="button"
          style={buttonStyle(disabledPrev)}
          disabled={disabledPrev}
          onClick={() => onPageChange(currentPage - 1)}
        >
          ‹ Prev
        </button>
        <span>
          Page <strong>{currentPage}</strong> of <strong>{totalPages}</strong>
        </span>
        <button
          type="button"
          style={buttonStyle(disabledNext)}
          disabled={disabledNext}
          onClick={() => onPageChange(currentPage + 1)}
        >
          Next ›
        </button>
        <button
          type="button"
          style={buttonStyle(disabledNext)}
          disabled={disabledNext}
          onClick={() => onPageChange(totalPages)}
        >
          Last »
        </button>
      </div>
    </div>
  );
}

export default Pagination;
