import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend

import matplotlib.pyplot as plt
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import json
import base64
import io
import os
import uuid
import re
from typing import Dict, List, Any, Optional
import logging
import seaborn as sns
import random
import numpy as np

# Suppress verbose logging from matplotlib and related libraries
logging.getLogger('matplotlib').setLevel(logging.WARNING)
logging.getLogger('matplotlib.font_manager').setLevel(logging.WARNING)
logging.getLogger('matplotlib.pyplot').setLevel(logging.WARNING)
logging.getLogger('PIL').setLevel(logging.WARNING)
logging.getLogger('fontTools').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)
# Directory to store generated charts
CHARTS_DIR = os.path.join(os.getcwd(), "generated_charts")
os.makedirs(CHARTS_DIR, exist_ok=True)

class ChartGenerator:
    def __init__(self):
        self.chart_counter = 0
        plt.style.use('default')
        sns.set_palette("husl")
        
    def parse_chart_requests(self, ai_response: str) -> List[Dict]:
        """
        Parse AI response for chart generation requests with improved JSON handling.
        Expected format: [CHART:type:title:data_json]
        """
        chart_requests = []
        
        # Use iterative approach to handle multiple charts properly
        # Find all chart start positions
        chart_starts = []
        pos = 0
        while True:
            pos = ai_response.find('[CHART:', pos)
            if pos == -1:
                break
            chart_starts.append(pos)
            pos += 7  # advance past '[CHART:' to avoid re-matching same token
        
        # Process each chart individually
        for i, start_pos in enumerate(chart_starts):
            try:
                # Find the end of this chart tag
                # Look for the closing ] that comes after the JSON
                search_start = start_pos + 7  # Skip "[CHART:"
                
                # Find the next chart start or end of string
                if i + 1 < len(chart_starts):
                    search_end = chart_starts[i + 1]
                else:
                    search_end = len(ai_response)
                
                # Extract the chart section
                chart_section = ai_response[start_pos:search_end]
                
                # Find the last ] in this section (should be our closing bracket)
                last_bracket = chart_section.rfind(']')
                if last_bracket == -1:
                    logger.warning(f"No closing bracket found for chart at position {start_pos}")
                    continue
                
                # Extract the complete chart tag
                chart_tag = chart_section[:last_bracket + 1]
                
                # Parse the chart tag components
                # Pattern: [CHART:type:title:data_json]
                if not chart_tag.startswith('[CHART:') or not chart_tag.endswith(']'):
                    logger.warning(f"Invalid chart tag format: {chart_tag[:50]}...")
                    continue
                
                # Remove [CHART: and ] and split by :
                content = chart_tag[7:-1]  # Remove '[CHART:' and ']'
                
                # Split into components (type:title:json_data)
                # Be careful with colons in the JSON
                parts = content.split(':', 2)  # Split into max 3 parts
                if len(parts) != 3:
                    logger.warning(f"Invalid chart tag structure: {chart_tag[:50]}...")
                    continue
                
                chart_type, title, data_str = parts
                data_str = data_str.strip()
                
                # Parse JSON data
                parsed_data = self._parse_json_safely(data_str)
                
                if parsed_data:
                    chart_requests.append({
                        'type': chart_type.lower().strip(),
                        'title': title.strip(),
                        'data': parsed_data,
                        'id': str(uuid.uuid4())[:8]
                    })
                    logger.info(f"Successfully parsed chart: {chart_type} - {title}")
                else:
                    logger.warning(f"Failed to parse chart JSON for '{title}': {data_str[:100]}...")
                    
            except Exception as e:
                logger.error(f"Error parsing chart at position {start_pos}: {e}")
                continue
                
        return chart_requests
    
    def _parse_json_safely(self, data_str: str) -> Optional[Dict]:
        """Try multiple strategies to parse potentially malformed JSON"""
        
        # Strategy 1: Direct JSON parsing
        try:
            return json.loads(data_str)
        except json.JSONDecodeError:
            pass
        
        # Strategy 2: Try to fix common JSON issues
        try:
            # Fix common issues like trailing commas, unquoted strings, etc.
            fixed_data = self._fix_common_json_issues(data_str)
            return json.loads(fixed_data)
        except json.JSONDecodeError:
            pass
        
        # Strategy 3: Extract data using regex patterns
        try:
            return self._extract_data_with_regex(data_str)
        except Exception:
            pass
        
        return None
    
    def _fix_common_json_issues(self, data_str: str) -> str:
        """Fix common JSON formatting issues"""
        # Remove any trailing content after incomplete JSON
        if data_str.count('{') > data_str.count('}'):
            # Find the last complete object
            brace_count = 0
            last_valid_pos = 0
            for i, char in enumerate(data_str):
                if char == '{':
                    brace_count += 1
                elif char == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        last_valid_pos = i + 1
                        break
            if last_valid_pos > 0:
                data_str = data_str[:last_valid_pos]
        
        # Fix incomplete arrays
        if data_str.count('[') > data_str.count(']'):
            data_str += ']' * (data_str.count('[') - data_str.count(']'))
        
        # Fix incomplete objects
        if data_str.count('{') > data_str.count('}'):
            data_str += '}' * (data_str.count('{') - data_str.count('}'))
        
        return data_str
    
    def _extract_data_with_regex(self, data_str: str) -> Dict:
        """Extract data using regex when JSON parsing fails"""
        result = {}
        
        # Extract labels/categories
        labels_match = re.search(r'"labels":\s*\[(.*?)\]', data_str, re.DOTALL)
        if labels_match:
            labels_str = labels_match.group(1)
            # Extract individual labels
            labels = re.findall(r'"([^"]+)"', labels_str)
            result['labels'] = labels
        
        # Extract values
        values_match = re.search(r'"values":\s*\[(.*?)\]', data_str, re.DOTALL)
        if values_match:
            values_str = values_match.group(1)
            # Extract numbers
            values = re.findall(r'[\d.]+', values_str)
            result['values'] = [float(v) for v in values if v]
        
        # Extract x data
        x_match = re.search(r'"x":\s*\[(.*?)\]', data_str, re.DOTALL)
        if x_match:
            x_str = x_match.group(1)
            x_data = re.findall(r'"([^"]+)"', x_str)
            result['x'] = x_data
        
        # Extract y data
        y_match = re.search(r'"y":\s*\[(.*?)\]', data_str, re.DOTALL)
        if y_match:
            y_str = y_match.group(1)
            y_values = re.findall(r'[\d.]+', y_str)
            result['y'] = [float(v) for v in y_values if v]
        
        # Extract axis labels
        xlabel_match = re.search(r'"xlabel":\s*"([^"]+)"', data_str)
        if xlabel_match:
            result['xlabel'] = xlabel_match.group(1)
            
        ylabel_match = re.search(r'"ylabel":\s*"([^"]+)"', data_str)
        if ylabel_match:
            result['ylabel'] = ylabel_match.group(1)
        
        return result if result else None
    
    def generate_chart(self, chart_config: Dict) -> Optional[str]:
        """
        Generate chart based on configuration and return relative path.
        """
        try:
            chart_type = chart_config['type']
            title = chart_config['title']
            data = chart_config['data']
            chart_id = chart_config['id']
            
            # Validate data first - if valid, don't fix it
            if self._validate_chart_data(data, chart_type):
                logger.info(f"Chart data validation passed for '{title}'")
            else:
                logger.warning(f"Chart data validation failed for '{title}': {data}")
                # Only apply fixes if validation fails
                data = self._fix_incomplete_data(data, chart_type)
                
                # Re-validate after fixing
                if not self._validate_chart_data(data, chart_type):
                    logger.error(f"Chart data still invalid after fixing for '{title}': {data}")
                    return None
            
            # Create filename
            safe_title = re.sub(r'[^\w\s-]', '', title).strip()
            safe_title = re.sub(r'[-\s]+', '-', safe_title)
            filename = f"chart_{chart_id}_{safe_title[:30]}.png"
            filepath = os.path.join(CHARTS_DIR, filename)
            
            # Generate chart based on type
            if chart_type in ['bar', 'column']:
                self._create_bar_chart(data, title, filepath)
            elif chart_type in ['line', 'trend']:
                self._create_line_chart(data, title, filepath)
            elif chart_type in ['pie', 'donut']:
                self._create_pie_chart(data, title, filepath)
            elif chart_type == 'scatter':
                self._create_scatter_chart(data, title, filepath)
            elif chart_type == 'histogram':
                self._create_histogram(data, title, filepath)
            else:
                # Default to bar chart
                self._create_bar_chart(data, title, filepath)
            
            # Return relative path for URL
            return f"generated_charts/{filename}"
            
        except Exception as e:
            logger.error(f"Error generating chart: {e}", exc_info=True)
            return None
    
    def _validate_chart_data(self, data: Dict, chart_type: str) -> bool:
        """Validate that data contains required fields for chart type"""
        if chart_type in ['bar', 'pie']:
            # Check for intended format: {"labels": [...], "values": [...]}
            has_simple_format = ('labels' in data and 'values' in data and 
                               len(data['labels']) == len(data['values']) and 
                               len(data['labels']) > 0)
            
            # Check for LLM's actual format: {"labels": [...], "series": [{"name": "...", "values": [...]}]}
            has_series_format = False
            if 'labels' in data and 'series' in data:
                series = data['series']
                if isinstance(series, list) and len(series) > 0:
                    # Check if first series has valid structure
                    first_series = series[0]
                    if isinstance(first_series, dict) and 'values' in first_series:
                        # For multi-series, we can use the first series length
                        # or we could validate that all series have same length
                        has_series_format = (len(data['labels']) > 0 and 
                                            len(first_series['values']) > 0)
            
            return has_simple_format or has_series_format
            
        elif chart_type in ['line', 'scatter']:
            if 'x' not in data or 'y' not in data or len(data['x']) == 0:
                return False
            
            y_data = data['y']
            
            # Case A: Simple list of numbers [10, 20, 30]
            if isinstance(y_data, list) and len(y_data) > 0 and isinstance(y_data[0], (int, float)):
                return len(data['x']) == len(y_data)
                
            # Case B: Multi-series list of dicts [{"label": "A", "values": [...]}]
            if isinstance(y_data, list) and len(y_data) > 0 and isinstance(y_data[0], dict):
                # Check if the first series has values matching X length
                first_series = y_data[0]
                return 'values' in first_series and len(first_series['values']) == len(data['x'])
                
            return False
            
        elif chart_type == 'histogram':
            return 'values' in data and len(data['values']) > 0
            
        return False
    
    def _fix_incomplete_data(self, data: Dict, chart_type: str) -> Dict:
        """Fix incomplete chart data by generating reasonable placeholder values"""
        fixed_data = data.copy()
        
        if chart_type in ['bar', 'pie']:
            # Need both labels and values
            if 'labels' in fixed_data and 'values' not in fixed_data and 'series' not in fixed_data:
                # Generate placeholder values based on number of labels
                num_labels = len(fixed_data['labels'])
                # Generate semi-realistic values (not completely random)
                base_values = [50, 75, 60, 80, 65, 90, 55, 70, 85, 45]
                fixed_data['values'] = [base_values[i % len(base_values)] + random.randint(-20, 30) 
                                      for i in range(num_labels)]
                logger.warning(f"Generated placeholder values for {chart_type} chart with {num_labels} labels")
            
        elif chart_type in ['line', 'scatter']:
            # Need both x and y
            if 'x' in fixed_data and 'y' not in fixed_data:
                # Generate placeholder y values
                num_x = len(fixed_data['x'])
                # Create a trend-like pattern
                base_trend = [i * 10 + 50 for i in range(num_x)]
                fixed_data['y'] = [val + random.randint(-15, 15) for val in base_trend]
                logger.warning(f"Generated placeholder y values for {chart_type} chart with {num_x} x points")
            
            if 'labels' in fixed_data and 'x' not in fixed_data:
                # Convert labels to x values
                fixed_data['x'] = fixed_data['labels']
                del fixed_data['labels']  # Remove labels since we're using x
                
        elif chart_type == 'histogram':
            if 'labels' in fixed_data and 'values' not in fixed_data:
                # For histogram, convert labels to values
                try:
                    # Try to extract numbers from labels
                    values = []
                    for label in fixed_data['labels']:
                        numbers = re.findall(r'[\d.]+', str(label))
                        if numbers:
                            values.extend([float(n) for n in numbers])
                    if values:
                        fixed_data['values'] = values
                        del fixed_data['labels']
                except:
                    # Fallback: generate random values
                    fixed_data['values'] = [random.randint(10, 100) for _ in range(20)]
        
        return fixed_data
    
    def _create_bar_chart(self, data: Dict, title: str, filepath: str):
        """Create bar chart using matplotlib - handles both simple and series formats"""
        plt.figure(figsize=(12, 8))
        
        labels = data['labels']
        
        # Handle both formats: simple {"labels": [...], "values": [...]} 
        # and series {"labels": [...], "series": [{"name": "...", "values": [...]}]}
        if 'values' in data:
            # Simple format
            values = data['values']
            rotation = 45 if len(labels) > 8 else 0
            bars = plt.bar(labels, values, color=sns.color_palette("husl", len(labels)))
            
            # Add value labels on bars
            for bar, value in zip(bars, values):
                plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(values)*0.01,
                        f'{value:.1f}', ha='center', va='bottom', fontsize=10)
                        
        elif 'series' in data:
            # Series format - multiple datasets
            series = data['series']
            x = np.arange(len(labels))
            width = 0.8 / len(series)  # Width of bars
            
            colors = sns.color_palette("husl", len(series))
            
            for i, series_data in enumerate(series):
                series_name = series_data.get('name', f'Series {i+1}')
                series_values = series_data['values']
                
                # Position bars side by side
                bar_positions = x + (i - len(series)/2 + 0.5) * width
                bars = plt.bar(bar_positions, series_values, width, 
                              label=series_name, color=colors[i])
                
                # Add value labels on bars
                for bar, value in zip(bars, series_values):
                    plt.text(bar.get_x() + bar.get_width()/2, 
                            bar.get_height() + max(series_values)*0.01,
                            f'{value:.1f}', ha='center', va='bottom', fontsize=9)
            
            plt.xticks(x, labels)
            plt.legend()
            rotation = 45 if len(labels) > 6 else 0
        
        plt.title(title, fontsize=16, fontweight='bold', pad=20)
        plt.xlabel(data.get('xlabel', ''), fontsize=12)
        plt.ylabel(data.get('ylabel', 'Values'), fontsize=12)
        
        plt.xticks(rotation=rotation, ha='right' if rotation > 0 else 'center')
        plt.grid(True, alpha=0.3, axis='y')
        plt.tight_layout()
        plt.savefig(filepath, dpi=150, bbox_inches='tight')
        plt.close()
    
    def _create_line_chart(self, data: Dict, title: str, filepath: str):
        """Create line chart using matplotlib (Supports Single and Multi-Series)"""
        plt.figure(figsize=(12, 8))
        
        x_data = data['x']
        y_data = data['y']
        
        # Check if Multi-Series (List of Dictionaries)
        if len(y_data) > 0 and isinstance(y_data[0], dict):
            # Iterate through series
            for series in y_data:
                # Handle 'label' (from your log) or 'name' keys
                label = series.get('label', series.get('name', 'Series'))
                values = series.get('values', [])
                
                # Ensure length matches x_data to prevent crashes
                if len(values) == len(x_data):
                    plt.plot(x_data, values, marker='o', linewidth=2, markersize=6, label=label)
            
            plt.legend() # Show legend for multi-series
        else:
            # Standard Single Series
            plt.plot(x_data, y_data, marker='o', linewidth=3, markersize=8)

        plt.title(title, fontsize=16, fontweight='bold', pad=20)
        plt.xlabel(data.get('xlabel', ''), fontsize=12)
        plt.ylabel(data.get('ylabel', 'Values'), fontsize=12)
        plt.grid(True, alpha=0.3)
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(filepath, dpi=150, bbox_inches='tight')
        plt.close()
    
    def _create_pie_chart(self, data: Dict, title: str, filepath: str):
        """Create pie chart using matplotlib"""
        plt.figure(figsize=(10, 10))
        
        labels = data['labels']
        values = data['values']
        
        colors = sns.color_palette("husl", len(labels))
        wedges, texts, autotexts = plt.pie(values, labels=labels, autopct='%1.1f%%', 
                                         colors=colors, startangle=90)
        plt.title(title, fontsize=16, fontweight='bold', pad=20)
        
        # Improve text readability
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
            autotext.set_fontsize(10)
        
        plt.axis('equal')
        plt.tight_layout()
        plt.savefig(filepath, dpi=150, bbox_inches='tight')
        plt.close()
    
    def _create_scatter_chart(self, data: Dict, title: str, filepath: str):
        """Create scatter plot using matplotlib"""
        plt.figure(figsize=(10, 8))
        
        x_data = data['x']
        y_data = data['y']
        
        plt.scatter(x_data, y_data, alpha=0.7, s=100)
        plt.title(title, fontsize=16, fontweight='bold', pad=20)
        plt.xlabel(data.get('xlabel', ''), fontsize=12)
        plt.ylabel(data.get('ylabel', 'Values'), fontsize=12)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(filepath, dpi=150, bbox_inches='tight')
        plt.close()
    
    def _create_histogram(self, data: Dict, title: str, filepath: str):
        """Create histogram using matplotlib"""
        plt.figure(figsize=(10, 8))
        
        values = data['values']
        bins = data.get('bins', min(20, len(values)//2))
        
        plt.hist(values, bins=bins, alpha=0.7, edgecolor='black')
        plt.title(title, fontsize=16, fontweight='bold', pad=20)
        plt.xlabel(data.get('xlabel', 'Values'), fontsize=12)
        plt.ylabel(data.get('ylabel', 'Frequency'), fontsize=12)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(filepath, dpi=150, bbox_inches='tight')
        plt.close()

# Global instance
chart_generator = ChartGenerator()

def process_charts_in_response(ai_response: str) -> str:
    """
    Process chart requests in AI response and replace with image tags.
    Also clean up any leftover JSON fragments.
    """
    chart_requests = chart_generator.parse_chart_requests(ai_response)
    
    if not chart_requests:
        logger.info("No chart requests found in response")
        # Even if no charts, clean up any JSON fragments
        return clean_json_fragments(ai_response)
    
    logger.info(f"Found {len(chart_requests)} chart requests")
    processed_response = ai_response
    
    for chart_config in chart_requests:
        # Generate the chart
        chart_path = chart_generator.generate_chart(chart_config)
        
        # Create the original chart tag pattern - be more flexible with matching
        chart_type = chart_config['type']
        title = chart_config['title']
        
        # Use a more flexible pattern to find and replace the chart tag
        pattern = re.escape(f'[CHART:{chart_type}:{title}:') + r'.*?\]'
        
        if chart_path:
            # Replace with HTML img tag
            img_tag = f'<div class="chart-container"><img src="/{chart_path}" alt="{title}" class="generated-chart" style="max-width: 95%; height: auto; margin: 10px 0;"><p class="chart-caption">{title}</p></div>'
            
            processed_response = re.sub(pattern, img_tag, processed_response, flags=re.DOTALL)
            logger.info(f"Generated chart: {chart_path} for '{title}'")
        else:
            # Remove the chart tag if generation failed
            processed_response = re.sub(pattern, f"<p><em>Gagal membuat grafik: {title}</em></p>", processed_response, flags=re.DOTALL)
            logger.warning(f"Failed to generate chart for '{title}'")
    
    # Clean up any remaining JSON fragments
    processed_response = clean_json_fragments(processed_response)
    
    return processed_response

def clean_json_fragments(text: str) -> str:
    """
    Remove leftover JSON fragments that weren't part of complete chart tags.
    """
    # Remove standalone JSON objects that start with common chart keys
    patterns_to_remove = [
        # Basic key-value patterns
        r',\s*"values":\s*\[[^\]]*\]\s*[,}]?',  # Leftover values arrays
        r',\s*"labels":\s*\[[^\]]*\]\s*[,}]?',  # Leftover labels arrays
        r',\s*"x":\s*\[[^\]]*\]\s*[,}]?',       # Leftover x arrays
        r',\s*"y":\s*\[[^\]]*\]\s*[,}]?',       # Leftover y arrays
        r',\s*"xlabel":\s*"[^"]*"\s*[,}]?',     # Leftover xlabel
        r',\s*"ylabel":\s*"[^"]*"\s*[,}]?',     # Leftover ylabel
        
        # Patterns that start with quotes (like the ones you're seeing)
        r'"xlabel":\s*"[^"]*"\s*[\]\}]?',       # "xlabel": "something"]
        r'"ylabel":\s*"[^"]*"\s*[\]\}]?',       # "ylabel": "something"]
        r'"values":\s*\[[^\]]*\]\s*[\]\}]?',    # "values": [...]"]
        r'"labels":\s*\[[^\]]*\]\s*[\]\}]?',    # "labels": [...]"]
        r'"x":\s*\[[^\]]*\]\s*[\]\}]?',         # "x": [...]"]
        r'"y":\s*\[[^\]]*\]\s*[\]\}]?',         # "y": [...]"]
        
        # More aggressive patterns for complex structures
        r'"[xy]label":\s*"[^"]*"[,\]\}]*',      # Any label with quotes and brackets
        r'"\w+":\s*\[[^\]]*\][,\]\}]*',         # Any key with array value
        r'^\s*"[^"]+"\s*:\s*[^,}\]]+\s*[\]\},]?\s*$',  # Standalone key-value pairs
        
        # Clean up trailing brackets and commas
        r'\s*[\]\}]+\s*$',                      # Trailing brackets
        r'^\s*,+\s*',                           # Leading commas
    ]
    
    cleaned_text = text
    for pattern in patterns_to_remove:
        cleaned_text = re.sub(pattern, '', cleaned_text, flags=re.MULTILINE)
    
    # Remove any remaining isolated JSON fragments (line-by-line cleaning)
    lines = cleaned_text.split('\n')
    filtered_lines = []
    
    for line in lines:
        original_line = line
        line_stripped = line.strip()
        
        # Skip lines that look like JSON fragments
        should_skip = False
        
        # Check for various JSON fragment patterns
        json_patterns = [
            r'^,?\s*"[^"]+"\s*:\s*"[^"]*"\s*[\]\}]?\s*$',  # "key": "value"]
            r'^,?\s*"[^"]+"\s*:\s*\[[^\]]*\]\s*[\]\}]?\s*$',  # "key": [array]]
            r'^[,\s]*"xlabel"',                           # Lines starting with "xlabel"
            r'^[,\s]*"ylabel"',                           # Lines starting with "ylabel"
            r'^[,\s]*"values"',                           # Lines starting with "values"
            r'^[,\s]*"labels"',                           # Lines starting with "labels"
            r'^\s*[\]\}]+\s*$',                           # Lines with only closing brackets
            r'^\s*,+\s*$',                                # Lines with only commas
        ]
        
        for pattern in json_patterns:
            if re.match(pattern, line_stripped):
                should_skip = True
                break
        
        # Also skip if line contains JSON structure but isn't HTML
        if (not should_skip and 
            not line_stripped.startswith('<') and 
            ('"values":' in line_stripped or 
             '"labels":' in line_stripped or 
             '"xlabel":' in line_stripped or 
             '"ylabel":' in line_stripped or
             line_stripped.endswith('}]') or
             line_stripped.endswith('"]'))):
            should_skip = True
        
        if not should_skip:
            filtered_lines.append(original_line)
    
    # Join lines and clean up excessive whitespace
    result = '\n'.join(filtered_lines)
    
    # Remove multiple consecutive empty lines
    result = re.sub(r'\n\s*\n\s*\n+', '\n\n', result)
    
    return result