use crate::state::AppState;
use crossterm::{
    event::{self, Event, KeyCode, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, Clear, Gauge, Paragraph, Sparkline, Wrap},
    Frame, Terminal,
};
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

pub fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;
    const TB: u64 = 1024 * GB;

    if bytes >= TB {
        format!("{:.2} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

#[derive(Default, Debug, Clone)]
pub struct CollectedMetrics {
    pub total_chunks: usize,
    pub live_chunks: usize,
    pub tombstoned_chunks: usize,
    pub total_users: usize,
    pub total_tokens: usize,
    pub ingested_bytes: u64,
    pub stored_bytes: u64,
    pub bytes_saved: u64,
    pub dedup_ratio: f64,
    pub packfiles_count: usize,
    pub gc_runs: u64,
    pub gc_freed_bytes: u64,
    pub http_requests_total: u64,
    pub http_status_2xx: u64,
    pub http_status_4xx: u64,
    pub http_status_5xx: u64,
}

impl CollectedMetrics {
    pub fn collect(state: &AppState) -> Self {
        let total_chunks = state.storage.chunk_count();
        let live_chunks = state.storage.live_chunk_count();
        let tombstoned_chunks = total_chunks.saturating_sub(live_chunks);

        let total_users = state.users.list_users().map(|u| u.len()).unwrap_or(0);
        let total_tokens = state.users.list_all_tokens().map(|t| t.len()).unwrap_or(0);

        let mut ingested_bytes = 0u64;
        let mut stored_bytes = 0u64;
        let mut gc_runs = 0u64;
        let mut gc_freed_bytes = 0u64;
        let mut packfiles_count = 1usize;
        let mut http_requests_total = 0u64;
        let mut http_status_2xx = 0u64;
        let mut http_status_4xx = 0u64;
        let mut http_status_5xx = 0u64;

        if let Some(handle) = &state.prometheus_handle {
            let rendered = handle.render();
            for line in rendered.lines() {
                if line.starts_with('#') || line.trim().is_empty() {
                    continue;
                }
                let mut parts = line.split_whitespace();
                let metric_name = parts.next().unwrap_or("");
                let metric_val = parts.next().and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);

                if metric_name.starts_with("uru_cas_bytes_ingested_raw_total") {
                    ingested_bytes = metric_val as u64;
                } else if metric_name.starts_with("uru_cas_bytes_stored_disk_total") {
                    stored_bytes = metric_val as u64;
                } else if metric_name.starts_with("uru_cas_gc_runs_total") {
                    gc_runs = metric_val as u64;
                } else if metric_name.starts_with("uru_cas_gc_bytes_freed_total") {
                    gc_freed_bytes = metric_val as u64;
                } else if metric_name.starts_with("uru_cas_packfiles_active_total") {
                    packfiles_count = metric_val as usize;
                } else if metric_name.starts_with("uru_cas_http_requests_total") {
                    let count = metric_val as u64;
                    http_requests_total += count;
                    if metric_name.contains("status=\"2") {
                        http_status_2xx += count;
                    } else if metric_name.contains("status=\"4") {
                        http_status_4xx += count;
                    } else if metric_name.contains("status=\"5") {
                        http_status_5xx += count;
                    }
                }
            }
        }

        let bytes_saved = ingested_bytes.saturating_sub(stored_bytes);
        let dedup_ratio = if ingested_bytes > 0 {
            (bytes_saved as f64 / ingested_bytes as f64) * 100.0
        } else {
            0.0
        };

        Self {
            total_chunks,
            live_chunks,
            tombstoned_chunks,
            total_users,
            total_tokens,
            ingested_bytes,
            stored_bytes,
            bytes_saved,
            dedup_ratio,
            packfiles_count,
            gc_runs,
            gc_freed_bytes,
            http_requests_total,
            http_status_2xx,
            http_status_4xx,
            http_status_5xx,
        }
    }
}

pub struct TuiApp {
    state: AppState,
    listen_addr: SocketAddr,
    start_time: Instant,
    metrics: CollectedMetrics,
    request_history: Vec<u64>,
    show_help: bool,
    gc_message: Option<(String, Instant)>,
}

impl TuiApp {
    pub fn new(state: AppState, listen_addr: SocketAddr) -> Self {
        let metrics = CollectedMetrics::collect(&state);
        Self {
            state,
            listen_addr,
            start_time: Instant::now(),
            metrics,
            request_history: vec![0; 40],
            show_help: false,
            gc_message: None,
        }
    }

    pub fn update(&mut self) {
        let prev_requests = self.metrics.http_requests_total;
        self.metrics = CollectedMetrics::collect(&self.state);
        let delta = self.metrics.http_requests_total.saturating_sub(prev_requests);

        self.request_history.push(delta);
        if self.request_history.len() > 60 {
            self.request_history.remove(0);
        }

        if let Some((_, timestamp)) = self.gc_message {
            if timestamp.elapsed() > Duration::from_secs(4) {
                self.gc_message = None;
            }
        }
    }

    pub fn trigger_gc(&mut self) {
        let policy = &self.state.retention_policy;
        match self.state.storage.run_gc(policy) {
            Ok(stats) => {
                let msg = format!(
                    "GC: Pruned {} vers, Tombstoned {} chunks, Reclaimed {} bytes in {}ms",
                    stats.versions_pruned,
                    stats.chunks_tombstoned,
                    stats.bytes_punched,
                    stats.duration_ms
                );
                self.gc_message = Some((msg, Instant::now()));
            }
            Err(e) => {
                self.gc_message = Some((format!("GC error: {e}"), Instant::now()));
            }
        }
    }
}

/// Run the Ratatui dashboard on the current terminal
pub async fn run_tui(
    state: AppState,
    listen_addr: SocketAddr,
    shutdown_tx: oneshot::Sender<()>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Setup terminal with panic safety
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen, crossterm::cursor::Hide)?;

    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        let _ = disable_raw_mode();
        let _ = execute!(std::io::stdout(), LeaveAlternateScreen, crossterm::cursor::Show);
        original_hook(panic_info);
    }));

    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = TuiApp::new(state, listen_addr);
    let mut last_tick = Instant::now();
    let tick_rate = Duration::from_millis(250);

    let mut shutdown_tx = Some(shutdown_tx);

    loop {
        terminal.draw(|f| ui_draw(f, &app))?;

        let timeout = tick_rate.saturating_sub(last_tick.elapsed());
        if event::poll(timeout)? {
            if let Event::Key(key) = event::read()? {
                if key.kind == event::KeyEventKind::Press {
                    match key.code {
                        KeyCode::Char('q') | KeyCode::Char('Q') | KeyCode::Esc => {
                            if let Some(tx) = shutdown_tx.take() {
                                let _ = tx.send(());
                            }
                            break;
                        }
                        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                            if let Some(tx) = shutdown_tx.take() {
                                let _ = tx.send(());
                            }
                            break;
                        }
                        KeyCode::Char('g') | KeyCode::Char('G') => {
                            app.trigger_gc();
                        }
                        KeyCode::Char('h') | KeyCode::Char('?') => {
                            app.show_help = !app.show_help;
                        }
                        _ => {}
                    }
                }
            }
        }

        if last_tick.elapsed() >= tick_rate {
            app.update();
            last_tick = Instant::now();
        }
    }

    // Restore terminal safely
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen, crossterm::cursor::Show)?;
    terminal.show_cursor()?;

    Ok(())
}

fn ui_draw(f: &mut Frame, app: &TuiApp) {
    let size = f.area();

    // Main layout: Title (3), Content (Min), Footer (3)
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(16),
            Constraint::Length(3),
        ])
        .split(size);

    // 1. Header Bar
    let uptime = app.start_time.elapsed();
    let uptime_str = format!(
        "{:02}h {:02}m {:02}s",
        uptime.as_secs() / 3600,
        (uptime.as_secs() % 3600) / 60,
        uptime.as_secs() % 60
    );

    let header_text = vec![Line::from(vec![
        Span::styled(" ⚡ UruDrive CAS Server ", Style::default().fg(Color::Cyan).bold()),
        Span::raw(" │ "),
        Span::styled(format!("http://{}", app.listen_addr), Style::default().fg(Color::Yellow)),
        Span::raw(" │ Status: "),
        Span::styled("● RUNNING", Style::default().fg(Color::Green).bold()),
        Span::raw(" │ Uptime: "),
        Span::styled(uptime_str, Style::default().fg(Color::White)),
    ])];

    let header = Paragraph::new(header_text).block(
        Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(Style::default().fg(Color::Cyan)),
    );
    f.render_widget(header, chunks[0]);

    // 2. Middle area: split horizontally into Left (Storage & Deduplication) and Right (Traffic & Protocols)
    let main_cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(chunks[1]);

    // Left Column: Storage Engine & Deduplication
    let left_rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(10), // CAS Chunks & Packfiles
            Constraint::Min(8),     // Deduplication & Disk Savings
        ])
        .split(main_cols[0]);

    // Left 1: CAS Chunks & Packfiles
    let chunks_text = vec![
        Line::from(vec![
            Span::styled("  Total Chunks Indexed:  ", Style::default().fg(Color::Gray)),
            Span::styled(format!("{:<10}", app.metrics.total_chunks), Style::default().fg(Color::White).bold()),
            Span::styled("Packfiles Active: ", Style::default().fg(Color::Gray)),
            Span::styled(format!("{}", app.metrics.packfiles_count), Style::default().fg(Color::Magenta).bold()),
        ]),
        Line::from(vec![
            Span::styled("  Live Chunks:           ", Style::default().fg(Color::Gray)),
            Span::styled(format!("{:<10}", app.metrics.live_chunks), Style::default().fg(Color::Green).bold()),
            Span::styled("Tombstoned:       ", Style::default().fg(Color::Gray)),
            Span::styled(format!("{}", app.metrics.tombstoned_chunks), Style::default().fg(Color::Yellow).bold()),
        ]),
        Line::from(vec![
            Span::styled("  FastCDC Chunk Size:    ", Style::default().fg(Color::Gray)),
            Span::styled("64 KB avg (16KB min / 256KB max)", Style::default().fg(Color::White)),
        ]),
        Line::from(vec![
            Span::styled("  Hash Algorithm:        ", Style::default().fg(Color::Gray)),
            Span::styled("BLAKE3-256 (0x1e tagged)", Style::default().fg(Color::Cyan)),
        ]),
        Line::from(vec![
            Span::styled("  Bloom Filter:          ", Style::default().fg(Color::Gray)),
            Span::styled("AtomicBloomFilter (FPR: 0.1%)", Style::default().fg(Color::Green)),
        ]),
        Line::from(vec![
            Span::styled("  Storage Path:          ", Style::default().fg(Color::Gray)),
            Span::styled(format!("{}", app.state.storage.root_dir().display()), Style::default().fg(Color::DarkGray)),
        ]),
    ];

    let chunks_widget = Paragraph::new(chunks_text).block(
        Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .title(" 📦 CAS Engine & Packfiles ")
            .border_style(Style::default().fg(Color::Blue)),
    );
    f.render_widget(chunks_widget, left_rows[0]);

    // Left 2: Deduplication Efficiency
    let dedup_rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(4), // Raw numbers
            Constraint::Length(3), // Gauge
            Constraint::Min(3),    // GC stats
        ])
        .split(left_rows[1]);

    let dedup_text = vec![
        Line::from(vec![
            Span::styled("  Ingested (Raw):  ", Style::default().fg(Color::Gray)),
            Span::styled(format!("{:<12}", format_bytes(app.metrics.ingested_bytes)), Style::default().fg(Color::Yellow).bold()),
            Span::styled("Stored (Disk): ", Style::default().fg(Color::Gray)),
            Span::styled(format!("{}", format_bytes(app.metrics.stored_bytes)), Style::default().fg(Color::White).bold()),
        ]),
        Line::from(vec![
            Span::styled("  Bandwidth Saved: ", Style::default().fg(Color::Gray)),
            Span::styled(format!("{:<12}", format_bytes(app.metrics.bytes_saved)), Style::default().fg(Color::Green).bold()),
            Span::styled("Dedup Ratio:   ", Style::default().fg(Color::Gray)),
            Span::styled(format!("{:.1}%", app.metrics.dedup_ratio), Style::default().fg(Color::Green).bold()),
        ]),
    ];

    let dedup_block = Block::default()
        .borders(Borders::TOP | Borders::LEFT | Borders::RIGHT)
        .border_type(BorderType::Rounded)
        .title(" 🚀 Global Deduplication Efficiency ")
        .border_style(Style::default().fg(Color::Green));
    let dedup_p = Paragraph::new(dedup_text).block(dedup_block);
    f.render_widget(dedup_p, dedup_rows[0]);

    // Gauge for deduplication savings
    let gauge_ratio = (app.metrics.dedup_ratio / 100.0).clamp(0.0, 1.0);
    let gauge = Gauge::default()
        .block(Block::default().borders(Borders::LEFT | Borders::RIGHT).border_style(Style::default().fg(Color::Green)))
        .gauge_style(Style::default().fg(Color::Green).bg(Color::DarkGray))
        .ratio(gauge_ratio)
        .label(format!("{:.1}% Network Transfer Saved", app.metrics.dedup_ratio));
    f.render_widget(gauge, dedup_rows[1]);

    // GC stats in bottom of dedup panel
    let gc_text = vec![Line::from(vec![
        Span::styled("  GC Cycles Run:   ", Style::default().fg(Color::Gray)),
        Span::styled(format!("{:<12}", app.metrics.gc_runs), Style::default().fg(Color::Cyan).bold()),
        Span::styled("Holes Punched: ", Style::default().fg(Color::Gray)),
        Span::styled(format!("{}", format_bytes(app.metrics.gc_freed_bytes)), Style::default().fg(Color::LightBlue).bold()),
    ])];
    let gc_block = Block::default()
        .borders(Borders::BOTTOM | Borders::LEFT | Borders::RIGHT)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::Green));
    let gc_p = Paragraph::new(gc_text).block(gc_block);
    f.render_widget(gc_p, dedup_rows[2]);

    // Right Column: Traffic, Protocols & User Accounts
    let right_rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(10), // Protocols & Accounts
            Constraint::Min(8),     // HTTP Traffic & Sparkline
        ])
        .split(main_cols[1]);

    // Right 1: Protocols & Accounts
    let proto_text = vec![
        Line::from(vec![
            Span::styled("  Custom CAS API:        ", Style::default().fg(Color::Gray)),
            Span::styled("Mounted at /api/v1 (Batch Wire Codec)", Style::default().fg(Color::White)),
        ]),
        Line::from(vec![
            Span::styled("  Amazon S3 Compatible:  ", Style::default().fg(Color::Gray)),
            Span::styled("Mounted at /s3 (s3s streaming)", Style::default().fg(Color::White)),
        ]),
        Line::from(vec![
            Span::styled("  WebDAV Filesystem:     ", Style::default().fg(Color::Gray)),
            Span::styled("Mounted at /dav (dav-server)", Style::default().fg(Color::White)),
        ]),
        Line::from(vec![
            Span::styled("  Prometheus Metrics:    ", Style::default().fg(Color::Gray)),
            Span::styled("Mounted at /metrics", Style::default().fg(Color::White)),
        ]),
        Line::from(vec![
            Span::styled("  Registered Users:      ", Style::default().fg(Color::Gray)),
            Span::styled(format!("{:<8}", app.metrics.total_users), Style::default().fg(Color::Yellow).bold()),
            Span::styled("Active API Tokens: ", Style::default().fg(Color::Gray)),
            Span::styled(format!("{}", app.metrics.total_tokens), Style::default().fg(Color::Yellow).bold()),
        ]),
    ];

    let proto_widget = Paragraph::new(proto_text).block(
        Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .title(" 🌐 Protocols & Authentication ")
            .border_style(Style::default().fg(Color::Magenta)),
    );
    f.render_widget(proto_widget, right_rows[0]);

    // Right 2: HTTP Traffic & Activity
    let traffic_rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Counters
            Constraint::Min(5),    // Sparkline
        ])
        .split(right_rows[1]);

    let traffic_text = vec![Line::from(vec![
        Span::styled("  Total Requests: ", Style::default().fg(Color::Gray)),
        Span::styled(format!("{:<8}", app.metrics.http_requests_total), Style::default().fg(Color::White).bold()),
        Span::styled("2xx: ", Style::default().fg(Color::Green)),
        Span::styled(format!("{:<6}", app.metrics.http_status_2xx), Style::default().fg(Color::Green).bold()),
        Span::styled("4xx: ", Style::default().fg(Color::Yellow)),
        Span::styled(format!("{:<6}", app.metrics.http_status_4xx), Style::default().fg(Color::Yellow).bold()),
        Span::styled("5xx: ", Style::default().fg(Color::Red)),
        Span::styled(format!("{}", app.metrics.http_status_5xx), Style::default().fg(Color::Red).bold()),
    ])];

    let traffic_block = Block::default()
        .borders(Borders::TOP | Borders::LEFT | Borders::RIGHT)
        .border_type(BorderType::Rounded)
        .title(" 📈 HTTP Request Throughput ")
        .border_style(Style::default().fg(Color::Yellow));
    let traffic_p = Paragraph::new(traffic_text).block(traffic_block);
    f.render_widget(traffic_p, traffic_rows[0]);

    // Sparkline of requests
    let sparkline_block = Block::default()
        .borders(Borders::BOTTOM | Borders::LEFT | Borders::RIGHT)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::Yellow));
    let sparkline = Sparkline::default()
        .block(sparkline_block)
        .data(&app.request_history)
        .style(Style::default().fg(Color::Yellow));
    f.render_widget(sparkline, traffic_rows[1]);

    // 3. Footer Bar
    let footer_text = if let Some((ref msg, _)) = app.gc_message {
        Line::from(vec![
            Span::styled(" 🔔 ", Style::default().fg(Color::Yellow).bold()),
            Span::styled(msg, Style::default().fg(Color::Cyan).bold()),
        ])
    } else {
        Line::from(vec![
            Span::styled(" [q/Esc] ", Style::default().fg(Color::Red).bold()),
            Span::raw("Quit Server  │ "),
            Span::styled(" [g] ", Style::default().fg(Color::Green).bold()),
            Span::raw("Trigger GC  │ "),
            Span::styled(" [h] ", Style::default().fg(Color::Yellow).bold()),
            Span::raw("Toggle Help  │ "),
            Span::styled(" Live Refresh: 250ms ", Style::default().fg(Color::DarkGray)),
        ])
    };

    let footer = Paragraph::new(vec![footer_text]).block(
        Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(Style::default().fg(Color::DarkGray)),
    );
    f.render_widget(footer, chunks[2]);

    // Help Modal Popup (if toggled)
    if app.show_help {
        render_help_modal(f, size);
    }
}

fn render_help_modal(f: &mut Frame, area: Rect) {
    let popup_area = centered_rect(60, 45, area);
    f.render_widget(Clear, popup_area);

    let help_text = vec![
        Line::from(Span::styled("UruDrive CAS Server Dashboard Help", Style::default().fg(Color::Cyan).bold())),
        Line::from(""),
        Line::from(vec![
            Span::styled("  q / Q / Esc  ", Style::default().fg(Color::Red).bold()),
            Span::raw("Cleanly stops the Axum server and exits"),
        ]),
        Line::from(vec![
            Span::styled("  g / G        ", Style::default().fg(Color::Green).bold()),
            Span::raw("Manually trigger mark-and-sweep GC & compaction"),
        ]),
        Line::from(vec![
            Span::styled("  h / ?        ", Style::default().fg(Color::Yellow).bold()),
            Span::raw("Toggle this help modal dialog"),
        ]),
        Line::from(vec![
            Span::styled("  Ctrl + C     ", Style::default().fg(Color::Red).bold()),
            Span::raw("Force immediate graceful shutdown"),
        ]),
        Line::from(""),
        Line::from(Span::styled("Mounted Protocols:", Style::default().fg(Color::White).bold())),
        Line::from("  • Custom CAS API:  POST /api/v1/chunks/batch, /files/commit"),
        Line::from("  • S3 API:          /s3/ (AWS S3 compatible PutObject/GetObject)"),
        Line::from("  • WebDAV:          /dav/ (Native virtual drive mount)"),
        Line::from("  • Metrics:         GET /metrics (Prometheus exposition)"),
        Line::from(""),
        Line::from(Span::styled("Press 'h' or 'Esc' to close.", Style::default().fg(Color::DarkGray))),
    ];

    let help_widget = Paragraph::new(help_text)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Double)
                .title(" 💡 Keyboard Shortcuts & Help ")
                .border_style(Style::default().fg(Color::Cyan)),
        )
        .wrap(Wrap { trim: true });

    f.render_widget(help_widget, popup_area);
}

fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}
