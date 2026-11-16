//! Whether a line is for the agent.

/// Words that do not make a line a question ("hey Pixel", "ok Pixel").
pub const FILLER: &[&str] = &[
    "hey", "hi", "hello", "ok", "okay", "so", "um", "uh", "yo", "excuse", "me",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Addressed {
    No,
    /// Only the name: wait for the question.
    NameOnly,
    Yes,
}

/// Whether `text` names the agent, and with more than the name. A wake word
/// is matched as a whole word, in any case; a line with at most one word that
/// is neither a wake word nor filler is only the name.
pub fn addressed(wake_words: &[String], respond_all: bool, text: &str) -> Addressed {
    if respond_all {
        return Addressed::Yes;
    }
    let words: Vec<String> = words(text).collect();
    let is_wake = |w: &String| wake_words.iter().any(|wake| wake == w);
    if !words.iter().any(is_wake) {
        return Addressed::No;
    }
    let rest = words
        .iter()
        .filter(|w| !is_wake(w) && !FILLER.contains(&w.as_str()))
        .count();
    if rest <= 1 {
        Addressed::NameOnly
    } else {
        Addressed::Yes
    }
}

/// The lowercased words of `text` (letters, digits and apostrophes).
pub fn words(text: &str) -> impl Iterator<Item = String> + '_ {
    text.split(|c: char| !c.is_alphanumeric() && c != '\'')
        .filter(|w| !w.is_empty())
        .map(str::to_lowercase)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn named(text: &str) -> Addressed {
        addressed(&["pixel".to_string()], false, text)
    }

    #[test]
    fn names_and_questions() {
        assert_eq!(named("Hey Pixel"), Addressed::NameOnly);
        assert_eq!(named("Pixel, what time is it?"), Addressed::Yes);
        assert_eq!(named("what time is it?"), Addressed::No);
        assert_eq!(named("Pixels everywhere"), Addressed::No);
        assert_eq!(named("ok Pixel thanks"), Addressed::NameOnly);
        assert_eq!(
            addressed(&["pixel".to_string()], true, "anything"),
            Addressed::Yes
        );
    }
}
