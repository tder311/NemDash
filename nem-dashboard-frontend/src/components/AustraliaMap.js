import React, { useEffect, useState, useRef, useCallback } from 'react';

const AustraliaMap = ({ darkMode, hoveredRegion, onRegionClick }) => {
  const [svgContent, setSvgContent] = useState('');
  const containerRef = useRef(null);

  useEffect(() => {
    // Load the SVG content
    fetch('/australia-map.svg')
      .then(response => response.text())
      .then(content => setSvgContent(content))
      .catch(error => console.error('Error loading SVG:', error));
  }, []);

  // Handle click events on state paths
  const handleStateClick = useCallback((event) => {
    const target = event.target;
    if (target.classList.contains('state-path') && onRegionClick) {
      const stateId = target.id;
      const regionCode = stateId.replace('state-', '');
      if (['NSW', 'VIC', 'QLD', 'SA', 'TAS'].includes(regionCode)) {
        onRegionClick(regionCode);
      }
    }
  }, [onRegionClick]);

  useEffect(() => {
    if (!containerRef.current || !svgContent) return;

    // Add click event listener to container
    const container = containerRef.current;
    container.addEventListener('click', handleStateClick);

    // Add pointer cursor to clickable state paths
    const allPaths = container.querySelectorAll('.state-path');
    allPaths.forEach(path => {
      const regionCode = path.id.replace('state-', '');
      if (['NSW', 'VIC', 'QLD', 'SA', 'TAS'].includes(regionCode)) {
        path.style.cursor = 'pointer';
      }
    });

    return () => {
      container.removeEventListener('click', handleStateClick);
    };
  }, [svgContent, handleStateClick]);

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