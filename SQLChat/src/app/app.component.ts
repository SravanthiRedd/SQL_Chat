import { Component, ElementRef, ViewChild, AfterViewInit, OnDestroy, NgZone, HostListener } from '@angular/core';
import { RouterOutlet } from '@angular/router';
import { FormsModule } from '@angular/forms';
import { CommonModule } from '@angular/common';
import { HttpClient } from '@angular/common/http';
import { Chart, ChartConfiguration, registerables } from 'chart.js';
import { Subject, Subscription } from 'rxjs';
import { debounceTime, distinctUntilChanged } from 'rxjs/operators';
import { WebhookService, WebhookResponse } from './services/webhook.service';
import { FastApiService } from './services/fastapi.service';
import * as XLSX from 'xlsx';
import jsPDF from 'jspdf';

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
  showChart = false;

  // Pagination
  currentPage = 1;
  pageSize = 10;

  // Email modal
  showEmailModal = false;
  emailTo = '';
  isSendingEmail = false;
  emailSent = false;
  emailError = '';

  // Export dropdown
  showExportMenu = false;
  showDrillExportMenu = false;
  private isDrillEmail = false;

  // Slice detail (click-through on chart)
  selectedSliceIndex: number | null = null;
  drillDownRows: any[][] = [];
  drillDownColumns: string[] = [];
  drillDownTitle = '';
  drillDownLoading = false;
  drillDownError = '';
  showDrillDown = false;
  drillDownSql = '';

  @HostListener('document:click', ['$event'])
  onDocumentClick(e: MouseEvent) {
    const target = e.target as HTMLElement;
    if (!target.closest('.export-dropdown'))       this.showExportMenu = false;
    if (!target.closest('.drill-export-dropdown')) this.showDrillExportMenu = false;
  }

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

  // Questions loaded from generated_questions.json
  private questionCorpus: string[] = [];


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

  private readonly chartKeywords = [
    'chart', 'graph', 'plot', 'pie', 'bar', 'line', 'doughnut', 'donut',
    'visualize', 'visualization', 'visual', 'show chart', 'show graph'
  ];

  get totalPages(): number {
    return Math.ceil(this.tableRows.length / this.pageSize);
  }

  get pagedRows(): any[][] {
    const start = (this.currentPage - 1) * this.pageSize;
    return this.tableRows.slice(start, start + this.pageSize);
  }

  get pageNumbers(): number[] {
    const total = this.totalPages;
    const current = this.currentPage;
    const delta = 2;
    const range: number[] = [];
    for (let i = Math.max(1, current - delta); i <= Math.min(total, current + delta); i++) {
      range.push(i);
    }
    return range;
  }

  get pagedEnd(): number {
    return Math.min(this.currentPage * this.pageSize, this.tableRows.length);
  }

  // ── Drill-down pagination ─────────────────────────────────────────
  drillPage = 1;
  readonly drillPageSize = 10;

  get drillTotalPages(): number {
    return Math.ceil(this.drillDownRows.length / this.drillPageSize);
  }

  get drillPagedRows(): any[][] {
    const start = (this.drillPage - 1) * this.drillPageSize;
    return this.drillDownRows.slice(start, start + this.drillPageSize);
  }

  get drillPageNumbers(): number[] {
    const total = this.drillTotalPages;
    const current = this.drillPage;
    const delta = 2;
    const range: number[] = [];
    for (let i = Math.max(1, current - delta); i <= Math.min(total, current + delta); i++) {
      range.push(i);
    }
    return range;
  }

  get drillPagedEnd(): number {
    return Math.min(this.drillPage * this.drillPageSize, this.drillDownRows.length);
  }

  drillGoToPage(page: number) {
    if (page >= 1 && page <= this.drillTotalPages) {
      this.drillPage = page;
    }
  }

  get isNumericResult(): boolean {
    const trimmed = this.textResult.trim();
    return trimmed !== '' && !isNaN(Number(trimmed));
  }

  userWantsChart(query: string): boolean {
    const q = query.toLowerCase();
    return this.chartKeywords.some(kw => q.includes(kw));
  }

  goToPage(page: number) {
    if (page >= 1 && page <= this.totalPages) {
      this.currentPage = page;
    }
  }

  constructor(private webhookService: WebhookService, private fastApiService: FastApiService, private ngZone: NgZone, private http: HttpClient) {}
  
  // ── Export ───────────────────────────────────────────────────────────────

  private get safeTitle(): string {
    return (this.chartTitle || 'export').replace(/[/\\?%*:|"<>]/g, '-');
  }

  // ---- Table exports ----

  exportCSV() {
    this.showExportMenu = false;
    const BOM  = '\ufeff';
    const header = this.tableColumns.map(c => this.csvCell(c)).join(',');
    const rows   = this.tableRows.map(row => row.map(cell => this.csvCell(cell)).join(',')).join('\n');
    this.downloadBlob(BOM + header + '\n' + rows, `${this.safeTitle}.csv`, 'text/csv;charset=utf-8;');
  }

  exportExcel() {
    this.showExportMenu = false;
    const ws = XLSX.utils.aoa_to_sheet([this.tableColumns, ...this.tableRows]);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, 'Data');
    // Style the header row
    const headerRange = XLSX.utils.decode_range(ws['!ref'] ?? 'A1');
    for (let c = headerRange.s.c; c <= headerRange.e.c; c++) {
      const addr = XLSX.utils.encode_cell({ r: 0, c });
      if (!ws[addr]) continue;
      ws[addr].s = { font: { bold: true }, fill: { fgColor: { rgb: '1A73E8' } } };
    }
    XLSX.writeFile(wb, `${this.safeTitle}.xlsx`);
  }

  exportTablePdf() {
    this.showExportMenu = false;
    const doc = new jsPDF({ orientation: 'landscape' });
    const title = this.safeTitle;
    doc.setFontSize(14);
    doc.setFont('helvetica', 'bold');
    doc.text(title, 14, 16);

    const cols = this.tableColumns;
    const rows = this.tableRows;
    const colW = Math.min(40, Math.floor(270 / cols.length));
    let y = 28;

    // Header
    doc.setFontSize(9);
    doc.setFillColor(26, 115, 232);
    doc.setTextColor(255, 255, 255);
    doc.rect(14, y - 5, colW * cols.length, 8, 'F');
    cols.forEach((col, i) => doc.text(String(col).substring(0, 14), 15 + i * colW, y));
    y += 8;

    // Rows
    doc.setTextColor(50, 50, 50);
    rows.forEach((row, ri) => {
      if (y > 195) { doc.addPage(); y = 15; }
      if (ri % 2 === 0) {
        doc.setFillColor(245, 249, 255);
        doc.rect(14, y - 5, colW * cols.length, 7, 'F');
      }
      doc.setFontSize(8);
      row.forEach((cell, i) => doc.text(String(cell ?? '').substring(0, 14), 15 + i * colW, y));
      y += 7;
    });

    doc.save(`${this.safeTitle}.pdf`);
  }

  // ---- Chart exports ----

  exportChartPng() {
    this.showExportMenu = false;
    const url = this.pieChart.nativeElement.toDataURL('image/png');
    const a   = document.createElement('a');
    a.href    = url;
    a.download = `${this.safeTitle}.png`;
    a.click();
  }

  exportChartCSV() {
    this.showExportMenu = false;
    const cols = this.tableColumns.length ? this.tableColumns : ['Label', 'Value'];
    const rows = this.tableRows.length
      ? this.tableRows
      : (this.currentChartData?.data.map(d => [d.label, d.value]) ?? []);
    const BOM    = '\ufeff';
    const header = cols.map(c => this.csvCell(c)).join(',');
    const body   = rows.map(row => row.map(cell => this.csvCell(cell)).join(',')).join('\n');
    this.downloadBlob(BOM + header + '\n' + body, `${this.safeTitle}.csv`, 'text/csv;charset=utf-8;');
  }

  exportChartExcel() {
    this.showExportMenu = false;
    const cols = this.tableColumns.length ? this.tableColumns : ['Label', 'Value'];
    const rows = this.tableRows.length
      ? this.tableRows
      : (this.currentChartData?.data.map(d => [d.label, d.value]) ?? []);
    const ws = XLSX.utils.aoa_to_sheet([cols, ...rows]);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, 'Chart Data');
    XLSX.writeFile(wb, `${this.safeTitle}.xlsx`);
  }

  exportChartPdf() {
    this.showExportMenu = false;
    const doc     = new jsPDF({ orientation: 'landscape' });
    const imgData = this.pieChart.nativeElement.toDataURL('image/png');
    const title   = this.safeTitle;

    // Chart image on page 1
    doc.setFontSize(14);
    doc.setFont('helvetica', 'bold');
    doc.text(title, 14, 16);
    doc.addImage(imgData, 'PNG', 14, 22, 265, 150);

    // Data table on page 2 (if real columns available)
    const cols = this.tableColumns.length ? this.tableColumns : [];
    const rows = this.tableRows.length    ? this.tableRows    : [];
    if (cols.length && rows.length) {
      doc.addPage();
      doc.setFontSize(12);
      doc.setFont('helvetica', 'bold');
      doc.text(title + ' — Data', 14, 14);

      const colW = Math.min(45, Math.floor(270 / cols.length));
      let y = 26;
      doc.setFontSize(9);
      doc.setFillColor(26, 115, 232);
      doc.setTextColor(255, 255, 255);
      doc.rect(14, y - 5, colW * cols.length, 8, 'F');
      cols.forEach((col, i) => doc.text(String(col).substring(0, 16), 15 + i * colW, y));
      y += 8;

      doc.setTextColor(50, 50, 50);
      rows.forEach((row, ri) => {
        if (y > 195) { doc.addPage(); y = 15; }
        if (ri % 2 === 0) { doc.setFillColor(245, 249, 255); doc.rect(14, y - 5, colW * cols.length, 7, 'F'); }
        doc.setFontSize(8);
        row.forEach((cell, i) => doc.text(String(cell ?? '').substring(0, 16), 15 + i * colW, y));
        y += 7;
      });
    }

    doc.save(`${title}.pdf`);
  }

  // ---- Text export ----

  exportText() {
    this.showExportMenu = false;
    const content = `${this.chartTitle}\n\n${this.textResult}`;
    this.downloadBlob(content, `${this.safeTitle}.txt`, 'text/plain;charset=utf-8;');
  }

  // ---- Drill-down exports ----

  private get drillSafeTitle(): string {
    return (this.drillDownTitle || 'drill-down').replace(/[/\\?%*:|"<>]/g, '-');
  }

  exportDrillCSV() {
    this.showDrillExportMenu = false;
    const BOM    = '\ufeff';
    const header = this.drillDownColumns.map(c => this.csvCell(c)).join(',');
    const rows   = this.drillDownRows.map(row => row.map(cell => this.csvCell(cell)).join(',')).join('\n');
    this.downloadBlob(BOM + header + '\n' + rows, `${this.drillSafeTitle}.csv`, 'text/csv;charset=utf-8;');
  }

  exportDrillExcel() {
    this.showDrillExportMenu = false;
    const ws = XLSX.utils.aoa_to_sheet([this.drillDownColumns, ...this.drillDownRows]);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, 'Details');
    XLSX.writeFile(wb, `${this.drillSafeTitle}.xlsx`);
  }

  exportDrillPdf() {
    this.showDrillExportMenu = false;
    const doc   = new jsPDF({ orientation: 'landscape' });
    const title = this.drillSafeTitle;
    doc.setFontSize(14);
    doc.setFont('helvetica', 'bold');
    doc.text(title, 14, 16);

    const cols = this.drillDownColumns;
    const rows = this.drillDownRows;
    const colW = Math.min(40, Math.floor(270 / cols.length));
    let y = 28;

    doc.setFontSize(9);
    doc.setFillColor(26, 115, 232);
    doc.setTextColor(255, 255, 255);
    doc.rect(14, y - 5, colW * cols.length, 8, 'F');
    cols.forEach((col, i) => doc.text(String(col).substring(0, 14), 15 + i * colW, y));
    y += 8;

    doc.setTextColor(50, 50, 50);
    rows.forEach((row, ri) => {
      if (y > 195) { doc.addPage(); y = 15; }
      if (ri % 2 === 0) {
        doc.setFillColor(245, 249, 255);
        doc.rect(14, y - 5, colW * cols.length, 7, 'F');
      }
      doc.setFontSize(8);
      row.forEach((cell, i) => doc.text(String(cell ?? '').substring(0, 14), 15 + i * colW, y));
      y += 7;
    });

    doc.save(`${title}.pdf`);
  }

  openDrillEmailModal() {
    this.isDrillEmail = true;
    this.openEmailModal();
  }

  private buildExcelBase64(columns: string[], rows: any[][], sheetName = 'Data'): string {
    const ws = XLSX.utils.aoa_to_sheet([columns, ...rows]);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, sheetName);
    return XLSX.write(wb, { bookType: 'xlsx', type: 'base64' });
  }

  private csvCell(value: any): string {
    return `"${String(value ?? '').replace(/"/g, '""')}"`;
  }

  private downloadBlob(content: string, filename: string, type: string) {
    const blob = new Blob([content], { type });
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement('a');
    a.href     = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
  }

  // ── Email modal ───────────────────────────────────────────────────────────

  openEmailModal() {
    this.emailTo    = '';
    this.emailSent  = false;
    this.emailError = '';
    this.showEmailModal = true;
  }

  closeEmailModal() {
    this.showEmailModal = false;
    this.emailTo        = '';
    this.emailError     = '';
    this.emailSent      = false;
    this.isDrillEmail   = false;
  }

  sendEmail() {
    if (!this.emailTo.trim()) return;
    this.isSendingEmail = true;
    this.emailError     = '';

    const subject = this.isDrillEmail
      ? (this.drillDownTitle || 'Details')
      : (this.chartTitle || 'Analytics Result');

    const payload: any = { to: this.emailTo.trim(), subject };

    if (this.isDrillEmail) {
      payload.file_type     = 'excel';
      payload.excel_base64  = this.buildExcelBase64(this.drillDownColumns, this.drillDownRows, 'Details');
      payload.excel_filename = `${this.drillSafeTitle}.xlsx`;
    } else if (this.responseFormat === 'table') {
      payload.file_type      = 'excel';
      payload.excel_base64   = this.buildExcelBase64(this.tableColumns, this.tableRows, 'Data');
      payload.excel_filename  = `${this.safeTitle}.xlsx`;
    } else if (this.showChart) {
      payload.file_type   = 'image';
      payload.chart_image = this.pieChart.nativeElement.toDataURL('image/png');
    } else if (this.responseFormat === 'text') {
      payload.file_type = 'text';
      payload.result    = this.textResult;
    }

    this.fastApiService.sendEmail(payload).subscribe({
      next: () => {
        this.isSendingEmail = false;
        this.emailSent      = true;
        this.isDrillEmail   = false;
      },
      error: (err) => {
        this.isSendingEmail = false;
        this.isDrillEmail   = false;
        this.emailError = err?.error?.detail || err?.message || 'Failed to send email.';
      }
    });
  }

  ngAfterViewInit() {
    this.createChart(this.defaultChartData);

    // Load questions from JSON asset
    this.http.get<string[]>('assets/generated_questions.json').subscribe({
      next: (questions) => { this.questionCorpus = questions; },
      error: () => { this.questionCorpus = []; }
    });

    // Debounced suggestion updates using local corpus
    this.suggestionSub = this.queryInput$.pipe(
      debounceTime(200),
      distinctUntilChanged()
    ).subscribe((q: string) => {
      if (q.trim().length < 2) {
        this.isFetchingSuggestions = false;
        this.showSuggestions = false;
        this.suggestions = [];
        return;
      }
      this.isFetchingSuggestions = false;
      this.suggestions = this.localFuzzy(q);
      this.showSuggestions = this.suggestions.length > 0;
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

  /** Local fuzzy matching against question corpus */
  private localFuzzy(query: string): SuggestionItem[] {
    const q = query.toLowerCase().trim();
    if (q.length < 2) return [];

    // Stop words to ignore when scoring
    const stopWords = new Set(['a','an','the','is','are','was','were','be','been','have','has','had',
      'do','does','did','will','would','could','should','may','might','of','in','on','at','to',
      'for','with','by','from','and','or','but','not','what','how','which','who','where','when']);

    const qWords = q.split(/\s+/).filter(w => w.length > 1 && !stopWords.has(w));

    const scored: { text: string; score: number }[] = [];

    for (const text of this.questionCorpus) {
      const tl = text.toLowerCase();

      // Exact substring match — highest priority
      if (tl.includes(q)) { scored.push({ text, score: 100 }); continue; }

      // Count how many query words appear in the question
      const tw = tl.split(/\s+/);
      let matchCount = 0;
      for (const w of qWords) {
        if (tw.some(t => t.includes(w) || w.includes(t))) matchCount++;
      }

      if (matchCount === 0) continue;

      const ratio = matchCount / qWords.length;
      scored.push({ text, score: ratio * 50 + matchCount });
    }

    return scored
      .sort((a, b) => b.score - a.score)
      .slice(0, 8)
      .map(s => ({ text: s.text, corrected: false, score: s.score }));
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

    // Show local fuzzy results immediately from question corpus
    this.suggestions = this.localFuzzy(q);
    this.showSuggestions = this.suggestions.length > 0;
    this.isFetchingSuggestions = false;

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
   * Handle search submission — match against generated_questions.json corpus
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
    this.currentPage = 1;
    this.showChart = this.userWantsChart(this.searchQuery);

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

        const serverWantsChart = !!parsed.format && parsed.format !== 'table' && parsed.format !== 'text';

        if (parsed.format === 'text') {
          const rows = parsed.tableRows ?? [];
          this.textResult = rows.length > 0 ? String(rows[0][0] ?? '') : '';
          if (parsed.title) this.chartTitle = parsed.title;
        } else if (parsed.format === 'table' && parsed.tableColumns?.length) {
          this.tableColumns = parsed.tableColumns;
          this.tableRows    = parsed.tableRows ?? [];
          if (parsed.title) this.chartTitle = parsed.title;
        } else if (this.showChart || serverWantsChart) {
          this.showChart    = true;   // ensure the chart section is visible
          this.tableColumns = parsed.tableColumns ?? [];
          this.tableRows    = parsed.tableRows    ?? [];
          if (chartItems && chartItems.length > 0) {
            this.createChart({
              title: parsed.title || this.chartTitle,
              type: (parsed.type as any) || this.currentChartType,
              data: chartItems
            });
          } else if (parsed.tableColumns?.length && parsed.tableRows?.length) {
            // chartData missing but rows are present — fall back to table view
            this.showChart    = false;
            this.responseFormat = 'table';
            this.tableColumns = parsed.tableColumns;
            this.tableRows    = parsed.tableRows;
            if (parsed.title) this.chartTitle = parsed.title;
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
  
  closeSliceDetail() {
  this.selectedSliceIndex = null;
  this.showDrillDown = false;
  this.drillDownRows = [];
  this.drillDownColumns = [];
  this.drillDownTitle = '';
  this.drillDownError = '';
  this.drillDownSql = '';
  this.drillPage = 1;
}
onChartSliceClick(index: number) {
  if (!this.currentChartData || index < 0) return;
  this.ngZone.run(() => {
    this.selectedSliceIndex = index;
    const clickedLabel = this.currentChartData!.data[index].label;
    const question = this.buildDrillDownQuestion(clickedLabel);
    if (!question) return;

    this.drillDownTitle   = `Details: ${clickedLabel}`;
    this.drillDownLoading = true;
    this.drillDownError   = '';
    this.drillDownRows    = [];
    this.drillDownColumns = [];
    this.drillDownSql     = '';
    this.drillPage        = 1;
    this.showDrillDown    = true;

    this.fastApiService.streamChatAndParseChart(question).subscribe({
      next: (parsed) => {
        this.ngZone.run(() => {
          this.drillDownLoading = false;
          this.drillDownSql     = parsed.sqlQuery ?? '';
          this.drillDownColumns = parsed.tableColumns ?? [];
          this.drillDownRows    = parsed.tableRows ?? [];
          if (!this.drillDownRows.length) {
            this.drillDownError = 'No records found.';
          }
        });
      },
      error: (err) => {
        this.ngZone.run(() => {
          this.drillDownLoading = false;
          this.drillDownError   = err.message || 'Failed to load details.';
        });
      }
    });
  });
}

private buildDrillDownQuestion(clickedLabel: string): string {
  return `The user previously asked: "${this.searchQuery}". 
The chart showed grouped results. 
Now the user clicked on "${clickedLabel}". 
Show the individual detail records for "${clickedLabel}" from the same data.
Return all relevant columns without aggregation.`;
}

// private buildDrillDownQuestion(clickedLabel: string): string {
//   const q = this.searchQuery.trim();

//   // 1. Extract the grouping dimension — the word(s) after "by / per / grouped by"
//   const byMatch = q.match(
//     /\b(?:by|per|group(?:ed)?\s+by)\s+([a-zA-Z][a-zA-Z0-9\s_]*?)(?=\s*(?:,|$|\b(?:and|where|with|in|for|order|limit|having)\b))/i
//   );
//   const dimension = byMatch?.[1]?.trim() ?? null;

//   // 2. Derive the subject entity by stripping chart/aggregation noise from the query
//   const entity = q
//     .replace(/\b(?:generate|show|display|create|get|find|list|give|produce|visualize)\b/gi, '')
//     .replace(/\b(?:a|an|the|me|all)\b/gi, '')
//     .replace(/\b(?:pie|bar|line|donut|doughnut|chart|graph|plot|visual(?:ization)?)\b/gi, '')
//     .replace(/\b(?:showing|for|of|with|about)\b/gi, '')
//     .replace(/\b(?:count|total|sum|number|amount|percentage|average|avg)\b/gi, '')
//     .replace(/\b(?:by|per|group(?:ed)?\s+by)\s+[a-zA-Z][a-zA-Z0-9\s_]*/gi, '')
//     .replace(/\s+/g, ' ')
//     .trim() || 'records';

//   if (dimension) {
//     return `List all ${entity} where ${dimension} is '${clickedLabel}'`;
//   }

//   // Fallback: append the filter to the original question as a refinement
//   return `From the context of "${q}", show all individual records for ${clickedLabel}`;
// }

  getSliceColor(index: number): string {
    if (!this.currentChartData) return this.defaultColors[index % this.defaultColors.length];
    const item = this.currentChartData.data[index];
    return item?.color || (this.currentChartData.colors?.[index]) || this.defaultColors[index % this.defaultColors.length];
  }

  getSlicePercentage(value: number): string {
    if (!this.currentChartData) return '0%';
    const total = this.currentChartData.data.reduce((sum, d) => sum + d.value, 0);
    return total > 0 ? ((value / total) * 100).toFixed(1) + '%' : '0%';
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
    this.selectedSliceIndex = null;

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
              this.onChartSliceClick(legendItem.index);
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
        },
        onClick: (event: any, elements: any[]) => {
          if (elements.length > 0) {
            this.onChartSliceClick(elements[0].index);
          }
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