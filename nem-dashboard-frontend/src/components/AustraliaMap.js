import React, { useEffect, useState, useRef, useCallback } from 'react';

const NEM_REGIONS = ['NSW', 'VIC', 'QLD', 'SA', 'TAS'];

const REGION_COLORS = {
  'NSW': '#1f77b4',
  'VIC': '#ff7f0e',
  'QLD': '#2ca02c',
  'SA': '#d62728',
  'TAS': '#9467bd'
};

const AustraliaMap = ({ darkMode, hoveredRegion, onRegionClick, onRegionHover, onRegionLeave }) => {
  const [svgContent, setSvgContent] = useState('');
  const [localHover, setLocalHover] = useState(null);
  const containerRef = useRef(null);

  useEffect(() => {
    fetch('/australia-map.svg')
      .then(response => response.text())
      .then(content => setSvgContent(content))
      .catch(error => console.error('Error loading SVG:', error));
  }, []);

  const handleStateClick = useCallback((event) => {
    const target = event.target;
    if (target.classList.contains('state-path') && onRegionClick) {
      const regionCode = target.id.replace('state-', '');
      if (NEM_REGIONS.includes(regionCode)) {
        onRegionClick(regionCode);
      }
    }
  }, [onRegionClick]);

  const handleMouseOver = useCallback((event) => {
    const target = event.target;
    if (target.classList.contains('state-path')) {
      const regionCode = target.id.replace('state-', '');
      if (NEM_REGIONS.includes(regionCode)) {
        setLocalHover(regionCode);
        if (onRegionHover) onRegionHover(regionCode);
      }
    }
  }, [onRegionHover]);

  const handleMouseOut = useCallback((event) => {
    const target = event.target;
    if (target.classList.contains('state-path')) {
      setLocalHover(null);
      if (onRegionLeave) onRegionLeave();
    }
  }, [onRegionLeave]);

  useEffect(() => {
    if (!containerRef.current || !svgContent) return;

    const container = containerRef.current;
    container.addEventListener('click', handleStateClick);
    container.addEventListener('mouseover', handleMouseOver);
    container.addEventListener('mouseout', handleMouseOut);

    const allPaths = container.querySelectorAll('.state-path');
    allPaths.forEach(path => {
      const regionCode = path.id.replace('state-', '');
      if (NEM_REGIONS.includes(regionCode)) {
        path.style.cursor = 'pointer';
      }
    });

    return () => {
      container.removeEventListener('click', handleStateClick);
      container.removeEventListener('mouseover', handleMouseOver);
      container.removeEventListener('mouseout', handleMouseOut);
    };
  }, [svgContent, handleStateClick, handleMouseOver, handleMouseOut]);

  // Highlight from either sidebar hover or direct map hover
  const activeRegion = hoveredRegion || localHover;

  useEffect(() => {
    if (!containerRef.current || !svgContent) return;

    const allPaths = containerRef.current.querySelectorAll('.state-path');
    allPaths.forEach(path => {
      path.classList.remove('highlighted');
      path.style.stroke = '';
      path.style.strokeWidth = '';
    });

    if (activeRegion) {
      const statePath = containerRef.current.querySelector(`#state-${activeRegion}`);
      if (statePath) {
        statePath.classList.add('highlighted');
        statePath.style.stroke = REGION_COLORS[activeRegion];
        statePath.style.strokeWidth = '3px';
      }
    }
  }, [activeRegion, svgContent]);

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