import { Component, ElementRef, ViewChild, AfterViewInit, OnDestroy, NgZone } from '@angular/core';
import { RouterOutlet } from '@angular/router';
import { FormsModule } from '@angular/forms';
import { CommonModule } from '@angular/common';
import { Chart, ChartConfiguration, registerables } from 'chart.js';
import { Subject, Subscription, of } from 'rxjs';
import { debounceTime, distinctUntilChanged, switchMap } from 'rxjs/operators';
import { WebhookService, WebhookResponse } from './services/webhook.service';
import { FastApiService } from './services/fastapi.service';

Chart.register(...registerables);

interface ChartDataItem {
  label: string;
  value: number;
  color?: string;
}

interface SuggestionItem {
  text: string;
  corrected: boolean;
  score: number;
}

interface ApiChartData {
  title?: string;
  type?: 'pie' | 'bar' | 'line' | 'doughnut' | 'donut';
  data: ChartDataItem[];
  colors?: string[];
  options?: any;
}

@Component({
  selector: 'app-root',
  imports: [RouterOutlet, FormsModule, CommonModule],
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css', './search.css']
})
export class AppComponent implements AfterViewInit, OnDestroy {
  title = 'ues-analytics-hub';
  
  @ViewChild('pieChart', { static: false }) pieChart!: ElementRef<HTMLCanvasElement>;
  
  searchQuery = '';
  webhookResponse: WebhookResponse | null = null;
  isLoading = false;
  currentChart: Chart | null = null;
  chartTitle = 'Revenue by Service Line';
  currentChartType: 'pie' | 'bar' | 'line' | 'doughnut' = 'pie';
  currentChartData: ApiChartData | null = null;
  resposeTille = '';

  // Response details shown in the info panel
  sqlQuery      = '';
  tablesUsed: string[] = [];

  // Table / text / chart display
  responseFormat = '';
  tableColumns: string[] = [];
  tableRows: any[][] = [];
  textResult = '';

  // ── Intellisense state ──────────────────────────────────────
  searchFocused         = false;
  showSuggestions       = false;
  activeIndex           = -1;
  suggestions: SuggestionItem[] = [];
  isFetchingSuggestions = false;
  suggestionTablesUsed: string[] = [];

  // Voice input state
  isListening = false;
  private recognition: any = null;

  // Drives debounced POST /suggest calls
  private queryInput$    = new Subject<string>();
  private suggestionSub!: Subscription;

  // Local fallback corpus (used when API is unavailable / query < 2 chars)
  private readonly fallbackCorpus: string[] = [
    'Show revenue by service line',
    'Show revenue circle chart',
    'Number of users per role',
    'Total work orders created per day',
    'Work orders by status',
    'Show monthly growth trend',
    'Show quarterly performance bar chart',
    'Users by department',
    'Top 10 clients by revenue',
    'Show environmental service data',
    'Geotechnical project summary',
    'Materials usage report',
    'Construction inspection count',
    'Show pipeline status',
    'HR headcount by role',
    'Show line chart for revenue',
    'Show donut chart for work orders',
    'Count of work orders per user',
    'Revenue by region',
    'Active projects count',
    'Show bar chart for users',
    'Monthly active users',
    'Work order completion rate',
    'Show pie chart for materials',
    'Field services summary',
  ];


  // Default chart data (fallback)
  private defaultChartData: ApiChartData = {
    title: 'Revenue by Service Line',
    type: 'pie',
    data: [
      { label: 'Construction Inspection', value: 25, color: '#E91E63' },
      { label: 'Environmental', value: 20, color: '#2196F3' },
      { label: 'Geotechnical', value: 20, color: '#9C27B0' },
      { label: 'Materials', value: 20, color: '#FF9800' },
      { label: 'Special Inspection', value: 15, color: '#795548' }
    ]
  };

  // Default color palette for dynamic charts
  private defaultColors = [
    '#E91E63', '#2196F3', '#9C27B0', '#FF9800', '#795548',
    '#4CAF50', '#FF5722', '#607D8B', '#009688', '#673AB7',
    '#FFC107', '#8BC34A', '#CDDC39', '#FFEB3B', '#F44336'
  ];

  // Chart type configurations
  private chartTypeConfigs = {
    pie: {
      legend: { 
        display: true, 
        position: 'right' as const,
        labels: {
          padding: 20,
          usePointStyle: true,
          font: { size: 12 },
          generateLabels: (chart: any) => this.generateLegendLabelsWithValues(chart)
        }
      },
      scales: undefined
    },
    doughnut: {
      legend: { 
        display: true, 
        position: 'right' as const,
        labels: {
          padding: 20,
          usePointStyle: true,
          font: { size: 12 },
          generateLabels: (chart: any) => this.generateLegendLabelsWithValues(chart)
        }
      },
      scales: undefined
    },
    bar: {
      legend: { 
        display: true,
        position: 'top' as const,
        labels: {
          padding: 20,
          usePointStyle: true,
          font: { size: 12 },
          generateLabels: (chart: any) => this.generateLegendLabelsWithValues(chart)
        }
      },
      scales: {
        y: {
          beginAtZero: true,
          grid: { color: '#e0e0e0' },
          ticks: { color: '#666' }
        },
        x: {
          grid: { display: false },
          ticks: { color: '#666' }
        }
      }
    },
    line: {
      legend: { 
        display: true,
        position: 'top' as const,
        labels: {
          padding: 20,
          usePointStyle: true,
          font: { size: 12 },
          generateLabels: (chart: any) => this.generateLegendLabelsWithValues(chart)
        }
      },
      scales: {
        y: {
          beginAtZero: true,
          grid: { color: '#e0e0e0' },
          ticks: { color: '#666' }
        },
        x: {
          grid: { display: false },
          ticks: { color: '#666' }
        }
      }
    }
  };

  constructor(private webhookService: WebhookService, private fastApiService: FastApiService, private ngZone: NgZone) {}
  
  ngAfterViewInit() {
    this.createChart(this.defaultChartData);
    this.testWebhookConnection();

    // Debounced POST /suggest — fires 400ms after user stops typing
    this.suggestionSub = this.queryInput$.pipe(
      debounceTime(400),
      distinctUntilChanged(),
      switchMap((q: string) => {
        if (q.trim().length < 2) {
          this.isFetchingSuggestions = false;
          this.showSuggestions = false;
          return of({ suggestions: [] as string[], tables_used: [] as string[], corrected: undefined });
        }
        this.isFetchingSuggestions = true;
        this.showSuggestions = true;
        return this.fastApiService.getSuggestions(q);
      })
    ).subscribe({
      next: (res) => {
        this.isFetchingSuggestions = false;
        this.suggestionTablesUsed = res.tables_used ?? [];

        if (res.suggestions && res.suggestions.length > 0) {
          this.suggestions = res.suggestions.map((text: string) => ({
            text,
            corrected: false,
            score: 0
          }));
        } else {
          // API returned nothing — fall back to local fuzzy
          this.suggestions = this.localFuzzy(this.searchQuery);
        }
        this.showSuggestions = !this.isLoading && this.suggestions.length > 0;
      },
      error: () => {
        this.isFetchingSuggestions = false;
        this.suggestions = this.localFuzzy(this.searchQuery);
        this.showSuggestions = !this.isLoading && this.suggestions.length > 0;
      }
    });
  }

  ngOnDestroy() {
    this.suggestionSub?.unsubscribe();
    this.queryInput$.complete();
  }

  /**
   * Test webhook connection on component initialization
   */
  testWebhookConnection() {
    console.log('Testing webhook connection...');
    this.webhookService.testConnection().subscribe({
      next: (response) => {
        console.log('Webhook connection successful:', response);
      },
      error: (error) => {
        console.error('Webhook connection failed:', error);
      }
    });
  }


  // ── Intellisense engine ───────────────────────────────────────────────────

  /** Levenshtein distance for spelling correction */
  private lev(a: string, b: string): number {
    const m = a.length, n = b.length;
    const d = Array.from({length: m+1}, (_,i) =>
      Array.from({length: n+1}, (_,j) => i===0 ? j : j===0 ? i : 0));
    for (let i=1;i<=m;i++)
      for (let j=1;j<=n;j++)
        d[i][j] = a[i-1]===b[j-1] ? d[i-1][j-1]
                  : 1+Math.min(d[i-1][j], d[i][j-1], d[i-1][j-1]);
    return d[m][n];
  }

  /** Local fuzzy fallback — used while API is in flight or unavailable */
  private localFuzzy(query: string): SuggestionItem[] {
    const q = query.toLowerCase().trim();
    if (q.length < 2) return [];
    const results: SuggestionItem[] = [];
    for (const text of this.fallbackCorpus) {
      const tl = text.toLowerCase();
      if (tl.startsWith(q))  { results.push({text, corrected:false, score:0}); continue; }
      if (tl.includes(q))    { results.push({text, corrected:false, score:1}); continue; }
      const qw = q.split(/\s+/), tw = tl.split(/\s+/);
      if (qw.every((w: string) => tw.some((t: string) => t.includes(w) || w.includes(t))))
        { results.push({text, corrected:false, score:2}); continue; }
      const maxD = (w: string) => w.length <= 3 ? 1 : w.length <= 6 ? 2 : 3;
      if (qw.every((w: string) => tw.some((t: string) => this.lev(w,t) <= maxD(w))))
        { results.push({text, corrected:true, score:3}); }
    }
    return results.sort((a,b) => a.score-b.score).slice(0,8);
  }

  /** The part the user already typed — shown normal weight */
  typedPart(suggestion: string): string {
    const q = this.searchQuery.toLowerCase();
    const sl = suggestion.toLowerCase();
    if (sl.startsWith(q)) return suggestion.slice(0, this.searchQuery.length);
    return '';
  }

  /** The remaining part — shown bold (Google style) */
  boldPart(suggestion: string): string {
    const q = this.searchQuery.toLowerCase();
    const sl = suggestion.toLowerCase();
    if (sl.startsWith(q)) return suggestion.slice(this.searchQuery.length);
    return suggestion;
  }

  onSearchFocus() {
    this.searchFocused = true;
    if (this.searchQuery.trim().length >= 1) this.showSuggestions = true;
  }

  onSearchBlur() {
    this.searchFocused = false;
    setTimeout(() => { this.showSuggestions = false; this.activeIndex = -1; }, 180);
  }

  onSearchInput(event: Event) {
    this.searchQuery = (event.target as HTMLInputElement).value;
    this.activeIndex = -1;

    const q = this.searchQuery.trim();

    if (q.length < 2) {
      this.suggestions = [];
      this.showSuggestions = false;
      this.isFetchingSuggestions = false;
      this.queryInput$.next('');
      return;
    }

    // 1. Show local fuzzy results immediately (instant feedback)
    this.suggestions = this.localFuzzy(q);
    this.showSuggestions = true;
    this.isFetchingSuggestions = true; // show pulsing dots while API is called

    // 2. Push to debounced stream → calls getSuggestions after 400ms idle
    this.queryInput$.next(q);
  }

  onSearchKeyDown(event: KeyboardEvent) {
    if (!this.showSuggestions) {
      if (event.key === 'Enter') this.onSearchSubmit();
      return;
    }
    switch (event.key) {
      case 'ArrowDown':
        event.preventDefault();
        this.activeIndex = Math.min(this.activeIndex+1, this.suggestions.length-1);
        break;
      case 'ArrowUp':
        event.preventDefault();
        this.activeIndex = Math.max(this.activeIndex-1, -1);
        break;
      case 'Enter':
        event.preventDefault();
        if (this.activeIndex >= 0) this.pickSuggestion(this.suggestions[this.activeIndex]);
        else { this.showSuggestions = false; this.onSearchSubmit(); }
        break;
      case 'Escape':
        this.showSuggestions = false;
        this.activeIndex = -1;
        break;
    }
  }

  pickSuggestion(item: SuggestionItem) {
    this.searchQuery = item.text;
    this.showSuggestions = false;
    this.activeIndex = -1;
  }

  fillInput(event: MouseEvent, text: string) {
    event.stopPropagation();
    this.searchQuery = text;
    this.suggestions = this.localFuzzy(text);
    this.queryInput$.next(text);
  }

  clearSearch() {
    this.searchQuery = '';
    this.suggestions = [];
    this.showSuggestions = false;
  }

  toggleVoiceInput() {
    if (this.isListening) {
      this.recognition?.stop();
      return;
    }

    const SpeechRecognition =
      (window as any).SpeechRecognition || (window as any).webkitSpeechRecognition;

    if (!SpeechRecognition) {
      alert('Voice input is not supported in this browser. Please use Chrome or Edge.');
      return;
    }

    this.recognition = new SpeechRecognition();
    this.recognition.continuous = false;
    this.recognition.interimResults = true;
    this.recognition.lang = 'en-US';

    this.recognition.onstart = () => {
      this.ngZone.run(() => { this.isListening = true; });
    };

    this.recognition.onresult = (event: any) => {
      const transcript = Array.from(event.results as SpeechRecognitionResultList)
        .map((r: any) => r[0].transcript)
        .join('');
      this.ngZone.run(() => { this.searchQuery = transcript; });
    };

    this.recognition.onerror = () => {
      this.ngZone.run(() => { this.isListening = false; });
    };
    this.recognition.onend = () => {
      this.ngZone.run(() => { this.isListening = false; });
    };

    this.recognition.start();
  }

  /**
   * Handle search submission — streams response from FastAPI /chat
   */
  onSearchSubmit() {
    if (!this.searchQuery.trim()) return;

    this.isLoading = true;
    this.webhookResponse = null;
    this.showSuggestions = false;
    this.sqlQuery = '';
    this.tablesUsed = [];
    this.responseFormat = '';
    this.tableColumns = [];
    this.tableRows = [];
    this.textResult = '';

    this.fastApiService.streamChatAndParseChart(this.searchQuery).subscribe({
      next: (parsed) => {
        this.isLoading = false;

        this.sqlQuery       = parsed.sqlQuery   ?? '';
        this.tablesUsed     = parsed.tablesUsed ?? [];
        this.responseFormat = parsed.format     ?? '';

        const chartItems = parsed.chartData;

        this.webhookResponse = {
          success: true,
          data: chartItems && chartItems.length > 0 ? chartItems : parsed.rawText
        };

        if (parsed.format === 'text') {
          // Single-value result: take first cell of first row
          const rows = parsed.tableRows ?? [];
          this.textResult = rows.length > 0 ? String(rows[0][0] ?? '') : '';
          if (parsed.title) this.chartTitle = parsed.title;
        } else if (parsed.format === 'table' && parsed.tableColumns?.length) {
          // Show as table
          this.tableColumns = parsed.tableColumns;
          this.tableRows    = parsed.tableRows ?? [];
          if (parsed.title) this.chartTitle = parsed.title;
        } else {
          // Show as chart (original behaviour)
          if (chartItems && chartItems.length > 0) {
            this.createChart({
              title: parsed.title || this.chartTitle,
              type: (parsed.type as any) || this.currentChartType,
              data: chartItems
            });
          }
        }
      },
      error: (err) => {
        this.isLoading = false;
        this.webhookResponse = { success: false, error: err.message };
      }
    });
  }



  /**
   * Handle webhook response that contains chart data
   */
  handleWebhookChartData(response: any) {
    try {
      // Check if the response contains chart data
      if (response.data) {
        const chartData = response.data;
        
        // Parse different possible data formats
        let apiChartData: ApiChartData;
        
        if (Array.isArray(chartData)) {
          // Format 1: Simple array of numbers [25, 20, 20, 20, 15]
          if (typeof chartData[0] === 'number') {
            apiChartData = this.parseNumberArray(chartData, response.data.labels, response.type || response.chartType);
          }
          // Format 2: Array of objects [{ label: 'Construction', value: 25 }]
          else if (typeof chartData[0] === 'object') {
            apiChartData = this.parseObjectArray(chartData, response.type || response.chartType);
          }
          else {
            throw new Error('Unsupported chart data format');
          }
        }
        // Format 3: Object with structured data
        else if (typeof chartData === 'object') {
          apiChartData = this.parseChartObject(chartData);
        }
        else {
          throw new Error('Invalid chart data format');
        }

        // Update chart title if provided
        if (response.title || chartData.title) {
          this.chartTitle = response.title || chartData.title;
        }

        // Create new chart with API data
        apiChartData.title = response.title;
        this.createChart(apiChartData);
        
        console.log('Chart updated with API data:', apiChartData);
      }
    } catch (error) {
      console.error('Error parsing chart data from API:', error);
      // Fallback to default chart
      this.createChart(this.defaultChartData);
    }
  }

  /**
   * Parse simple number array format
   */
  parseNumberArray(values: number[], labels?: string[], chartType?: string): ApiChartData {
    const defaultLabels = [
      'Construction Inspection', 'Environmental', 'Geotechnical', 
      'Materials', 'Special Inspection', 'Other'
    ];
    
    const chartLabels = labels || defaultLabels.slice(0, values.length);
    
    return {
      title: this.chartTitle,
      type: (chartType as any) || 'pie',
      data: values.map((value, index) => ({
        label: chartLabels[index] || `Item ${index + 1}`,
        value: value,
        color: this.defaultColors[index % this.defaultColors.length]
      }))
    };
  }

  /**
   * Parse object array format
   */
  parseObjectArray(items: any[], chartType?: string): ApiChartData {
    return {
      title: this.chartTitle,
      type: (chartType as any) || 'pie',
      data: items.map((item, index) => ({
        label: item.label || item.name || item.category || `Item ${index + 1}`,
        value: item.value || item.amount || item.percentage || 0,
        color: item.color || this.defaultColors[index % this.defaultColors.length]
      }))
    };
  }

  /**
   * Parse structured chart object format
   */
  parseChartObject(chartObj: any): ApiChartData {
    // Normalize chart type
    let chartType = chartObj.type || 'pie';
    if (chartType === 'donut') chartType = 'doughnut';
    
    return {
      title: chartObj.title || this.chartTitle,
      type: chartType as 'pie' | 'bar' | 'line' | 'doughnut',
      data: chartObj.data || chartObj.items || [],
      colors: chartObj.colors || this.defaultColors,
      options: chartObj.options
    };
  }

  /**
   * Request revenue chart data from webhook
   */
  requestRevenueData() {
    this.webhookService.requestChartData('revenue-by-service-line').subscribe({
      next: (response) => {
        console.log('Revenue data response:', response);
        if (response.success && response.data) {
          this.handleWebhookChartData(response.data);
        }
      },
      error: (error) => {
        console.error('Failed to get revenue data:', error);
      }
    });
  }

  /**
   * Handle Enter key press in search input
   */
  onSearchKeyPress(event: KeyboardEvent) {
    if (event.key === 'Enter') {
      this.onSearchSubmit();
    }
  }
  
  /**
   * Create chart dynamically from API data (supports multiple chart types)
   * @param apiData - Chart data from API response
   */
  createChart(apiData: ApiChartData) {
    const ctx = this.pieChart.nativeElement.getContext('2d');
    
    if (!ctx) {
      console.error('Canvas context not available');
      return;
    }

    // Store current chart data for type switching
    this.currentChartData = apiData;

    // Destroy existing chart if it exists
    if (this.currentChart) {
      this.currentChart.destroy();
    }

    // Normalize chart type
    let chartType = apiData.type || 'pie';
    if (chartType === 'donut') chartType = 'doughnut';

    // Update current chart type
    this.currentChartType = chartType as 'pie' | 'bar' | 'line' | 'doughnut';

    // Prepare chart data
    const labels = apiData.data.map(item => item.label);
    const values = apiData.data.map(item => item.value);
    const colors = apiData.data.map((item, index) => 
      item.color || (apiData.colors && apiData.colors[index]) || this.defaultColors[index % this.defaultColors.length]
    );

    // Update chart title
    this.chartTitle = apiData.title || 'Chart Data';

    // Get chart type configuration
    const typeConfig = this.chartTypeConfigs[chartType as keyof typeof this.chartTypeConfigs] || this.chartTypeConfigs.pie;

    // Create dataset based on chart type
    const dataset = this.createDataset(chartType as any, values, colors);

    const config: ChartConfiguration = {
      type: chartType as any,
      data: {
        labels: labels,
        datasets: [dataset]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: {
            ...typeConfig.legend,
            onClick: (event: any, legendItem: any, legend: any) => {
              // Handle legend click to toggle data visibility
              const chart = legend.chart;
              const index = legendItem.index;
              
              if (chartType === 'pie' || chartType === 'doughnut') {
                // For pie/doughnut charts, toggle segment visibility
                const meta = chart.getDatasetMeta(0);
                meta.data[index].hidden = !meta.data[index].hidden;
              } else {
                // For bar/line charts, toggle dataset visibility
                const dataset = chart.data.datasets[0];
                const meta = chart.getDatasetMeta(0);
                
                if (meta.data[index]) {
                  meta.data[index].hidden = !meta.data[index].hidden;
                }
              }
              
              chart.update();
            }
          },
          tooltip: {
            callbacks: {
              label: (context) => {
                const label = context.label || '';
                const value = context.parsed;
                
                // Handle different chart types for tooltip
                if (chartType === 'pie' || chartType === 'doughnut') {
                  const dataset = context.dataset.data;
                  const total = dataset.reduce((a: number, b: any) => {
                    const numValue = typeof b === 'number' ? b : 0;
                    return a + numValue;
                  }, 0);
                  const percentage = total > 0 ? ((value / total) * 100).toFixed(1) : '0.0';
                  return `${label}: ${value} (${percentage}%)`;
                } else {
                  // For bar and line charts
                  const yValue = typeof value === 'object' && value !== null && 'y' in value ? (value as any).y : value;
                  return `${label}: ${yValue}`;
                }
              }
            }
          }
        },
        scales: typeConfig.scales,
        animation: {
          duration: 1000
        }
      }
    };
    
    // Create new chart
    this.currentChart = new Chart(ctx, config);
    
    console.log('Chart created with data:', {
      type: chartType,
      labels,
      values,
      colors,
      title: this.chartTitle
    });
  }

  /**
   * Create dataset configuration based on chart type
   */
  createDataset(chartType: 'pie' | 'bar' | 'line' | 'doughnut', values: number[], colors: string[]) {
    const baseDataset = {
      data: values,
      borderWidth: 2,
      borderColor: '#ffffff'
    };

    switch (chartType) {
      case 'pie':
      case 'doughnut':
        return {
          ...baseDataset,
          backgroundColor: colors,
          hoverBorderWidth: 3,
          hoverBorderColor: '#ffffff'
        };
      
      case 'bar':
        return {
          ...baseDataset,
          label: 'Values', // Add label for legend
          backgroundColor: colors.map(color => color + '80'), // Add transparency
          borderColor: colors,
          borderWidth: 1,
          hoverBackgroundColor: colors,
          hoverBorderWidth: 2
        };
      
      case 'line':
        return {
          ...baseDataset,
          label: 'Trend', // Add label for legend
          backgroundColor: colors[0] + '20', // First color with transparency
          borderColor: colors[0],
          borderWidth: 3,
          fill: true,
          tension: 0.4,
          pointBackgroundColor: colors[0],
          pointBorderColor: '#ffffff',
          pointBorderWidth: 2,
          pointRadius: 6,
          pointHoverRadius: 8
        };
      
      default:
        return baseDataset;
    }
  }

  /**
   * Generate custom legend labels with values for all chart types
   */
  generateLegendLabelsWithValues(chart: any) {
    const data = chart.data;
    if (!data.labels || !data.datasets || !data.datasets[0]) return [];

    const dataset = data.datasets[0];
    const chartType = chart.config.type;

    return data.labels.map((label: string, index: number) => {
      const value = dataset.data[index];
      const color = Array.isArray(dataset.backgroundColor)
        ? dataset.backgroundColor[index]
        : dataset.backgroundColor;

      // For pie/doughnut: "2025-07-01 (34)", for bar/line: "Label: value"
      let displayText = '';
      if (chartType === 'pie' || chartType === 'doughnut') {
        displayText = `${label} (${value})`;
      } else {
        displayText = `${label}: ${value}`;
      }

      return {
        text: displayText,
        fillStyle: color,
        strokeStyle: color,
        lineWidth: 0,
        pointStyle: 'circle',
        hidden: false,
        index: index
      };
    });
  }

  /**
   * Change chart type and recreate chart with current data
   */
  changeChartType(newType: 'pie' | 'bar' | 'line' | 'doughnut') {
    this.currentChartType = newType;
    
    if (this.currentChartData) {
      // Update the chart data type and recreate
      const updatedData = { ...this.currentChartData, type: newType };
      this.createChart(updatedData);
    } else {
      // Use default data with new type
      const updatedData = { ...this.defaultChartData, type: newType };
      this.createChart(updatedData);
    }
    
    console.log(`Chart type changed to: ${newType}`);
  }

  /**
   * Refresh chart with new data (can be called externally)
   */
  refreshChart(newData?: ApiChartData) {
    const dataToUse = newData || this.defaultChartData;
    this.createChart(dataToUse);
  }
}