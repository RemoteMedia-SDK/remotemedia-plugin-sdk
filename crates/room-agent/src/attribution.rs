//! Who said a line.

use remotemedia_traits::{emit::Origin, NodeRuntimeContextRead};

/// The name of an unattributed line's author.
pub const SOMEONE: &str = "Someone";

/// `participant.name` when the client set one, else the origin id without
/// a page's `-xxxx` (four hex digits) suffix.
pub fn speaker_name(origin: Option<&Origin>) -> String {
    let Some(origin) = origin else {
        return SOMEONE.into();
    };
    if let Some(name) = origin
        .fields()
        .get("participant.name")
        .filter(|n| !n.is_empty())
    {
        return name.clone();
    }
    strip_tag(origin.id()).to_string()
}

/// A room member's name for a line's author: the display name the room
/// gave, else as [`speaker_name`].
pub fn room_speaker_name(origin: Option<&Origin>) -> String {
    origin
        .and_then(|origin| {
            origin
                .fields()
                .get("participant.display_name")
                .filter(|n| !n.is_empty())
                .cloned()
        })
        .unwrap_or_else(|| speaker_name(origin))
}

/// Whether an agent-authored line is this agent's own: the author's name is
/// `name`, or the origin id is `name` or `member_id`, in any case.
pub fn is_own(origin: Option<&Origin>, author: &str, name: &str, member_id: &str) -> bool {
    let same = |a: &str, b: &str| a.to_lowercase() == b.to_lowercase();
    same(author, name)
        || origin.is_some_and(|origin| same(origin.id(), name) || same(origin.id(), member_id))
}

/// [`speaker_name`] of the call's origin.
pub fn ctx_speaker_name(ctx: &dyn NodeRuntimeContextRead) -> String {
    speaker_name(ctx.origin().as_deref())
}

/// [`room_speaker_name`] of the call's origin.
pub fn ctx_room_speaker_name(ctx: &dyn NodeRuntimeContextRead) -> String {
    room_speaker_name(ctx.origin().as_deref())
}

fn strip_tag(id: &str) -> &str {
    match id.rsplit_once('-') {
        Some((name, tag)) if tag.len() == 4 && tag.chars().all(|c| c.is_ascii_hexdigit()) => name,
        _ => id,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn origin(fields: &[(&str, &str)]) -> Origin {
        let metadata: HashMap<String, String> = fields
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Origin::from_metadata(&metadata).unwrap()
    }

    #[test]
    fn names() {
        assert_eq!(speaker_name(None), "Someone");
        let page = origin(&[("participant.id", "ana-1f3a")]);
        assert_eq!(speaker_name(Some(&page)), "ana");
        let not_a_tag = origin(&[("participant.id", "ana-room")]);
        assert_eq!(speaker_name(Some(&not_a_tag)), "ana-room");
        let named = origin(&[("participant.id", "x-1f3a"), ("participant.name", "Ana")]);
        assert_eq!(speaker_name(Some(&named)), "Ana");
        let displayed = origin(&[
            ("participant.id", "x-1f3a"),
            ("participant.name", "Ana"),
            ("participant.display_name", "Ana B."),
        ]);
        assert_eq!(room_speaker_name(Some(&displayed)), "Ana B.");
        assert_eq!(room_speaker_name(Some(&named)), "Ana");
    }

    #[test]
    fn own_line_after_a_takeover() {
        let agent = origin(&[
            ("participant.id", "pixel-agent"),
            ("participant.display_name", "pixel"),
        ]);
        let author = room_speaker_name(Some(&agent));
        assert!(is_own(Some(&agent), &author, "Pixel", "pixel-agent"));
        // By member id alone.
        let renamed = origin(&[
            ("participant.id", "PIXEL-AGENT"),
            ("participant.display_name", "Assistant"),
        ]);
        assert!(is_own(Some(&renamed), "Assistant", "Pixel", "pixel-agent"));
        let other = origin(&[
            ("participant.id", "sage-agent"),
            ("participant.display_name", "Sage"),
        ]);
        assert!(!is_own(Some(&other), "Sage", "Pixel", "pixel-agent"));
    }
}
