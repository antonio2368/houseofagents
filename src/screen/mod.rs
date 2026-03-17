pub mod help;
pub mod home;
pub mod memory;
pub mod order;
pub mod pipeline;
pub mod prompt;
pub mod results;
pub mod running;

use crate::app::{App, Screen};
use ratatui::layout::Rect;
use ratatui::style::{Color, Style};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};
use ratatui::Frame;

pub fn draw(f: &mut Frame, app: &App) {
    match app.screen {
        Screen::Home => home::draw(f, app),
        Screen::Prompt => prompt::draw(f, app),
        Screen::Order => order::draw(f, app),
        Screen::Running => running::draw(f, app),
        Screen::Results => results::draw(f, app),
        Screen::Pipeline => pipeline::draw(f, app),
        Screen::Memory => memory::draw(f, app),
    }

    // Global modal overlay — rendered on every screen, always topmost
    if let Some(ref msg) = app.error_modal {
        draw_modal(f, msg, " Error ", Color::Red);
    } else if let Some(ref msg) = app.info_modal {
        draw_modal(f, msg, " Info ", Color::Green);
    }
}

fn draw_modal(f: &mut Frame, message: &str, title: &str, color: Color) {
    let area = centered_rect(60, 20, f.area());
    f.render_widget(Clear, area);
    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(color));
    let inner = block.inner(area);
    f.render_widget(block, area);
    let msg = Paragraph::new(message)
        .style(Style::default().fg(color))
        .wrap(Wrap { trim: false });
    f.render_widget(msg, inner);
}

pub fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let w = (area.width as u32 * percent_x.min(100) as u32 / 100) as u16;
    let h = (area.height as u32 * percent_y.min(100) as u32 / 100) as u16;
    let x = area.x + (area.width.saturating_sub(w)) / 2;
    let y = area.y + (area.height.saturating_sub(h)) / 2;
    Rect::new(x, y, w, h)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn centered_rect_returns_expected_size() {
        let area = Rect::new(0, 0, 100, 40);
        let centered = centered_rect(60, 50, area);
        assert_eq!(centered.width, 60);
        assert_eq!(centered.height, 20);
    }

    #[test]
    fn centered_rect_is_positioned_in_center() {
        let area = Rect::new(0, 0, 80, 24);
        let centered = centered_rect(50, 50, area);
        assert_eq!(centered.x, 20);
        assert_eq!(centered.y, 6);
    }

    #[test]
    fn centered_rect_no_overflow_on_large_terminal() {
        // A 700-column terminal would overflow u16 with `700 * 100`
        let area = Rect::new(0, 0, 700, 200);
        let centered = centered_rect(80, 80, area);
        assert_eq!(centered.width, 560);
        assert_eq!(centered.height, 160);
    }
}
