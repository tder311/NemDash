import React, { useEffect, useState, useRef } from 'react';

const AustraliaMap = ({ darkMode, hoveredRegion }) => {
  const [svgContent, setSvgContent] = useState('');
  const containerRef = useRef(null);

  useEffect(() => {
    // Load the SVG content
    fetch('/australia-map.svg')
      .then(response => response.text())
      .then(content => setSvgContent(content))
      .catch(error => console.error('Error loading SVG:', error));
  }, []);

  useEffect(() => {
    if (!containerRef.current || !svgContent) return;

    // Clear any existing highlighting
    const allPaths = containerRef.current.querySelectorAll('.state-path');
    allPaths.forEach(path => {
      path.classList.remove('highlighted');
    });

    // Highlight the hovered region if there is one
    if (hoveredRegion) {
      const stateId = `state-${hoveredRegion}`;
      const statePath = containerRef.current.querySelector(`#${stateId}`);

      if (statePath) {
        statePath.classList.add('highlighted');
        console.log(`Highlighted region: ${hoveredRegion} (${stateId})`);
      } else {
        console.warn(`Could not find state path for region: ${hoveredRegion} (${stateId})`);
      }
    }
  }, [hoveredRegion, svgContent]);

  const containerStyle = {
    position: 'absolute',
    top: '50%',
    left: '50%',
    transform: 'translate(-50%, -50%)',
    width: '90%',
    height: '90%',
    zIndex: 1,
    opacity: 0.4,
    filter: darkMode ? 'invert(1) hue-rotate(180deg)' : 'none',
    transition: 'all 0.3s ease'
  };

  return (
    <div
      ref={containerRef}
      style={containerStyle}
      dangerouslySetInnerHTML={{ __html: svgContent }}
    />
  );
};

export default AustraliaMap;