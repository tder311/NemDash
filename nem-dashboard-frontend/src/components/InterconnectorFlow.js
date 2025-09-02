import React from 'react';
import './InterconnectorFlow.css';

function InterconnectorFlow({ fromRegion, toRegion, flow, positions, darkMode }) {
  const fromPos = positions[fromRegion];
  const toPos = positions[toRegion];
  
  if (!fromPos || !toPos) return null;

  // Calculate positions (approximate center of cards)
  const getCardCenter = (pos) => {
    let x = 70; // Default center
    let y = 50; // Default center
    
    if (pos.left) x = parseFloat(pos.left) + 7; // Card width/2 in percentage
    if (pos.right) x = 100 - parseFloat(pos.right) - 7;
    if (pos.top) y = parseFloat(pos.top) + 5; // Card height/2 in percentage  
    if (pos.bottom) y = 100 - parseFloat(pos.bottom) - 5;
    
    return { x, y };
  };

  const from = getCardCenter(fromPos);
  const to = getCardCenter(toPos);
  
  // Calculate line properties
  const dx = to.x - from.x;
  const dy = to.y - from.y;
  const length = Math.sqrt(dx * dx + dy * dy);
  const angle = Math.atan2(dy, dx) * 180 / Math.PI;
  
  // Determine flow direction and color
  let flowColor, actualFlow, direction;
  if (flow > 0) {
    flowColor = '#10b981'; // green
    actualFlow = flow;
    direction = 'forward';
  } else if (flow < 0) {
    flowColor = '#ef4444'; // red  
    actualFlow = Math.abs(flow);
    direction = 'reverse';
  } else {
    flowColor = '#6b7280'; // gray
    actualFlow = 0;
    direction = 'none';
  }

  // Calculate midpoint for flow label
  const midX = (from.x + to.x) / 2;
  const midY = (from.y + to.y) / 2;

  return (
    <div className="interconnector-flow">
      {/* Connection line */}
      <div
        className="flow-line"
        style={{
          position: 'absolute',
          left: `${from.x}%`,
          top: `${from.y}%`,
          width: `${length}%`,
          height: '3px',
          backgroundColor: flowColor,
          transformOrigin: '0 50%',
          transform: `rotate(${angle}deg)`,
          opacity: 0.7,
          transition: 'all 0.3s ease'
        }}
      />
      
      {/* Arrow indicator */}
      {direction !== 'none' && (
        <div
          className="flow-arrow"
          style={{
            position: 'absolute',
            left: `${direction === 'forward' ? to.x - 1 : from.x - 1}%`,
            top: `${direction === 'forward' ? to.y : from.y}%`,
            width: '0',
            height: '0',
            borderLeft: `8px solid ${direction === 'forward' ? flowColor : 'transparent'}`,
            borderRight: `8px solid ${direction === 'reverse' ? flowColor : 'transparent'}`,
            borderTop: '8px solid transparent',
            borderBottom: '8px solid transparent',
            transform: `translate(-50%, -50%) rotate(${direction === 'forward' ? angle : angle + 180}deg)`,
            transition: 'all 0.3s ease'
          }}
        />
      )}
      
      {/* Flow value label */}
      <div
        className={`flow-label ${darkMode ? 'dark' : 'light'}`}
        style={{
          position: 'absolute',
          left: `${midX}%`,
          top: `${midY - 2}%`,
          transform: 'translate(-50%, -50%)',
          backgroundColor: flowColor,
          color: 'white',
          padding: '2px 8px',
          borderRadius: '12px',
          fontSize: '0.75rem',
          fontWeight: 'bold',
          whiteSpace: 'nowrap',
          boxShadow: '0 2px 4px rgba(0,0,0,0.2)',
          transition: 'all 0.3s ease'
        }}
      >
        {actualFlow.toFixed(0)}MW
      </div>
    </div>
  );
}

export default InterconnectorFlow;