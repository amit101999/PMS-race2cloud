import React from "react";

/**
 * The workflow progress bar shown at the top of Transaction Upload and Holding
 * Comparison. Steps before `currentStage` are done (filled, ticked); the
 * current step is a ring with a dot. Styles are the .txo-tracker rules in
 * TempTransaction.css.
 *
 * @param {{ steps: Array<{key: string, label: string}>, currentStage: number, label: string }} props
 */
const WorkflowTracker = ({ steps, currentStage, label }) => (
    <ol className="txo-tracker" aria-label={label}>
        {steps.map((step, i) => {
            const reached = i <= currentStage;
            return (
                <li
                    key={step.key}
                    className={
                        i === currentStage ? "is-current" : i < currentStage ? "is-done" : "is-pending"
                    }
                >
                    <div className="txo-tracker-row">
                        {i !== 0 ? (
                            <span className={reached ? "txo-tracker-line is-filled" : "txo-tracker-line"} />
                        ) : null}
                        <span className="txo-tracker-node" aria-hidden="true">
                            {/* Only finished steps are ticked; the current one is marked as "you are here". */}
                            {i < currentStage ? (
                                <svg width="9" height="7" viewBox="0 0 9 7" fill="none">
                                    <path
                                        d="M1 3.5L3.2 5.7L8 1"
                                        stroke="white"
                                        strokeWidth="1.4"
                                        strokeLinecap="round"
                                        strokeLinejoin="round"
                                    />
                                </svg>
                            ) : null}
                        </span>
                        {i !== steps.length - 1 ? (
                            <span
                                className={i < currentStage ? "txo-tracker-line is-filled" : "txo-tracker-line"}
                            />
                        ) : (
                            <span className="txo-tracker-line is-spacer" />
                        )}
                    </div>
                    <span className="txo-tracker-label">{step.label}</span>
                </li>
            );
        })}
    </ol>
);

export default WorkflowTracker;
