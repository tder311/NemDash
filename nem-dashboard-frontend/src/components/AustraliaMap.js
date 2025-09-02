import React, { useEffect, useState } from 'react';

const AustraliaMap = ({ darkMode, hoveredRegion }) => {
  const [svgContent, setSvgContent] = useState('');

  useEffect(() => {
    // Load the SVG content
    fetch('/australia-map.svg')
      .then(response => response.text())
      .then(content => setSvgContent(content))
      .catch(error => console.error('Error loading SVG:', error));
  }, []);

  const containerStyle = {
    position: 'absolute',
    top: 0,
    left: 0,
    width: '100%',
    height: '100%',
    zIndex: 1,
    opacity: 0.4,
    filter: darkMode ? 'invert(1) hue-rotate(180deg)' : 'none',
    transition: 'all 0.3s ease'
  };

  return (
    <div 
      style={containerStyle}
      dangerouslySetInnerHTML={{ __html: svgContent }}
    />
  );
};

export default AustraliaMap;