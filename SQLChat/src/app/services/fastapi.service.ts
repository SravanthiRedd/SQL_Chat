import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Observable, Subject, of } from 'rxjs';
import { catchError, map } from 'rxjs/operators';

export interface ChatRequest {
  message: string;
}

export interface ChartDataItem {
  label: string;
  value: number;
  color?: string;
}

export interface SuggestionResponse {
  suggestions: string[];
  tables_used?: string[];
  corrected?: string;
}

export interface ParsedChartResponse {
  title?: string;
  type?: string;
  format?: string;
  chartData?: ChartDataItem[];
  tableColumns?: string[];
  tableRows?: any[][];
  tableCount?: number;
  rawText?: string;
  sqlQuery?: string;
  tablesUsed?: string[];
  statusMessages?: string[];
}

@Injectable({
  providedIn: 'root'
})
export class FastApiService {
  private baseUrl = 'http://localhost:8000';

  constructor(private http: HttpClient) {}

  /**
   * POST /suggest — calls the FastAPI LLM backend to get 5 smart suggestions.
   * Falls back to empty list on error so local fuzzy takes over.
   */
  getSuggestions(query: string): Observable<SuggestionResponse> {
    const headers = new HttpHeaders({ 'Content-Type': 'application/json' });
    return this.http
      .post<SuggestionResponse>(`${this.baseUrl}/suggest`, { query }, { headers })
      .pipe(
        map(res => ({
          suggestions: Array.isArray(res.suggestions) ? res.suggestions : [],
          tables_used: res.tables_used,
          corrected: undefined
        })),
        catchError(() => of({ suggestions: [], corrected: undefined }))
      );
  }

  /**
   * Send message to FastAPI /chat endpoint and stream the response.
   * Emits each SSE chunk as a string via the returned Subject.
   * The subject completes when the stream ends.
   */
  streamChat(message: string): Subject<string> {
    const subject = new Subject<string>();

    fetch(`${this.baseUrl}/chat`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Accept': 'text/event-stream' },
      body: JSON.stringify({ message })
    })
      .then(response => {
        if (!response.ok) {
          subject.error(new Error(`HTTP ${response.status}: ${response.statusText}`));
          return;
        }

        const reader = response.body!.getReader();
        const decoder = new TextDecoder();

        const read = () => {
          reader.read().then(({ done, value }) => {
            if (done) {
              subject.complete();
              return;
            }

            const chunk = decoder.decode(value, { stream: true });

            // Parse SSE lines: "data: <payload>\n\n"
            chunk.split('\n').forEach(line => {
              if (line.startsWith('data: ')) {
                const payload = line.slice(6).trim();
                if (payload && payload !== '[DONE]') {
                  subject.next(payload);
                }
              }
            });

            read();
          }).catch(err => subject.error(err));
        };

        read();
      })
      .catch(err => subject.error(err));

    return subject;
  }

  /**
   * Accumulate full streamed response and try to parse chart data from it.
   * Returns an Observable that emits the parsed result once streaming completes.
   */
  streamChatAndParseChart(message: string): Observable<ParsedChartResponse> {
    return new Observable(observer => {
      let fullText = '';
      const stream = this.streamChat(message);

      stream.subscribe({
        next: chunk => { fullText += chunk; },
        error: err => observer.error(err),
        complete: () => {
          observer.next(this.parseChartFromText(fullText));
          observer.complete();
        }
      });
    });
  }

  /**
   * Parse chart data from LLM response text.
   * The stream is multiple concatenated JSON objects — find the "result" chunk.
   */
  parseChartFromText(text: string): ParsedChartResponse {
    const result: ParsedChartResponse = { rawText: text };

    try {
      const chunks = this.splitJsonObjects(text);

      // Extract SQL query from {"type":"sql","text":"..."} chunk
      const sqlChunk = chunks.find(c => c.type === 'sql');
      if (sqlChunk?.text) {
        result.sqlQuery = sqlChunk.text;
      }

      // Extract tables used
      const tablesChunk = chunks.find(c => c.type === 'tables_used');
      if (tablesChunk?.tables) {
        result.tablesUsed = tablesChunk.tables;
      }

      // Extract format hint (table vs chart)
      const formatChunk = chunks.find(c => c.type === 'format');
      if (formatChunk?.format) {
        result.format = formatChunk.format;
      }

      // Extract status messages
      result.statusMessages = chunks
        .filter(c => c.type === 'status' && c.text)
        .map(c => c.text);

      // Find the result chunk with rows/columns
      const resultChunk = chunks.find(c => c.type === 'result');
      if (resultChunk && Array.isArray(resultChunk.rows) && Array.isArray(resultChunk.columns)) {
        const columns: string[] = resultChunk.columns;
        const rows: any[][]     = resultChunk.rows;

        result.title       = resultChunk.title;
        result.type        = resultChunk.chart_type;
        result.tableColumns = columns;
        result.tableRows    = rows;
        result.tableCount   = resultChunk.count;

        // Single numeric value → show as text/KPI card
        if (rows.length === 1 && columns.length === 1 && !isNaN(Number(rows[0][0]))) {
          result.format = 'text';
          return result;
        }

        // Build chart data for all formats except 'table' and 'text'
        if (result.format !== 'table' && result.format !== 'text') {
          // label = first column, value = first numeric column after index 0
          const labelIndex = 0;
          let valueIndex = columns.findIndex((_, i) => i > 0 && rows.some(r => !isNaN(Number(r[i]))));
          if (valueIndex < 0) valueIndex = columns.length > 1 ? 1 : 0;

          // If label and value are the same column, treat as table instead
          if (valueIndex === labelIndex) {
            result.format = 'table';
          } else {
            result.chartData = rows.map((row: any[]) => ({
              label: String(row[labelIndex] ?? ''),
              value: Number(row[valueIndex] ?? 0)
            }));
          }
        }
        return result;
      }

      // Fallback: try parsing the whole text as a single JSON
      const parsed = JSON.parse(text.trim());
      if (Array.isArray(parsed)) {
        result.chartData = parsed.map((item: any) => ({
          label: item.label || item.date || item.name || '',
          value: Number(item.value || item.count || item.amount || 0),
          color: item.color
        }));
      } else if (parsed && typeof parsed === 'object') {
        result.title = parsed.title;
        result.type  = parsed.type || parsed.chartType;
        const items  = parsed.chartData || parsed.data || parsed.items || [];
        result.chartData = items.map((item: any) => ({
          label: item.label || item.date || item.name || '',
          value: Number(item.value || item.count || item.amount || 0),
          color: item.color
        }));
      }
    } catch {
      // Not parseable — rawText only
    }

    return result;
  }

  /**
   * Split a string of concatenated JSON objects into parsed objects.
   * e.g. '{"a":1}{"b":2}' → [{a:1}, {b:2}]
   */
  private splitJsonObjects(text: string): any[] {
    const results: any[] = [];
    let depth = 0;
    let start = -1;

    for (let i = 0; i < text.length; i++) {
      const ch = text[i];
      if (ch === '{') {
        if (depth === 0) start = i;
        depth++;
      } else if (ch === '}') {
        depth--;
        if (depth === 0 && start !== -1) {
          try {
            results.push(JSON.parse(text.slice(start, i + 1)));
          } catch { /* skip malformed chunk */ }
          start = -1;
        }
      }
    }

    return results;
  }

  /**
   * Simple non-streaming POST to /chat (fallback).
   */
  sendMessage(message: string): Observable<any> {
    const headers = new HttpHeaders({ 'Content-Type': 'application/json' });
    return this.http.post(`${this.baseUrl}/chat`, { message }, { headers });
  }

  /**
   * POST /send-email — sends result data (table, text, or chart image) via email.
   */
  sendEmail(payload: {
    to: string;
    subject: string;
    format: string;
    sql_query?: string;
    tables_used?: string[];
    columns?: string[];
    rows?: any[][];
    result?: string;
    chart_image?: string;
    chart_type?: string;
  }): Observable<any> {
    const headers = new HttpHeaders({ 'Content-Type': 'application/json' });
    const n8nWebhook = 'https://aihub.astmcompliance.ai/n8n/webhook/send-Email';
    return this.http.post(n8nWebhook, payload, { headers });
  }
}
