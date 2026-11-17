//! Sentences out of streamed text.

/// Splits streamed text into sentences as they complete: a sentence ends at
/// `.`, `!` or `?` followed by whitespace, or at a newline. Options:
///
/// * [`first_clause`](Self::first_clause): the first piece may end at a comma
///   once it is this many bytes long, so speech can start sooner;
/// * [`abbreviation_guard`](Self::abbreviation_guard): no cut after a short
///   capitalised word ("Dr.", "U.S.");
/// * [`long_clause_marks`](Self::long_clause_marks): a piece longer than 60
///   bytes may also end at `:` or `;`;
/// * [`max_bytes`](Self::max_bytes): [`push`](Self::push) fails once more than
///   this much text was pushed in all.
#[derive(Clone, Debug, Default)]
pub struct Sentences {
    pending: String,
    first_clause: usize,
    abbreviation_guard: bool,
    long_clause_marks: bool,
    max_bytes: Option<usize>,
    pushed: usize,
    emitted: bool,
}

/// [`Sentences::push`] went over its byte bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Overflow;

impl std::fmt::Display for Overflow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("text exceeds the output bound")
    }
}

impl std::error::Error for Overflow {}

impl Sentences {
    /// Plain splitting: no option set.
    pub fn new() -> Self {
        Self::default()
    }

    /// The splitter a spoken assistant uses: the first piece may end at a
    /// comma after 24 bytes, with the abbreviation guard and the long-clause
    /// marks.
    pub fn spoken() -> Self {
        Self::new()
            .first_clause(24)
            .abbreviation_guard(true)
            .long_clause_marks(true)
    }

    /// 0 turns it off.
    pub fn first_clause(mut self, bytes: usize) -> Self {
        self.first_clause = bytes;
        self
    }

    pub fn abbreviation_guard(mut self, on: bool) -> Self {
        self.abbreviation_guard = on;
        self
    }

    pub fn long_clause_marks(mut self, on: bool) -> Self {
        self.long_clause_marks = on;
        self
    }

    pub fn max_bytes(mut self, bytes: usize) -> Self {
        self.max_bytes = Some(bytes);
        self
    }

    /// Adds text; returns the sentences it completed.
    pub fn push(&mut self, text: &str) -> Result<Vec<String>, Overflow> {
        self.pushed += text.len();
        if self.max_bytes.is_some_and(|max| self.pushed > max) {
            return Err(Overflow);
        }
        self.pending.push_str(text);
        let mut out = Vec::new();
        while let Some(at) = self.cut() {
            let sentence = self.pending[..at].trim().to_string();
            self.pending.drain(..at);
            if !sentence.is_empty() {
                self.emitted = true;
                out.push(sentence);
            }
        }
        Ok(out)
    }

    /// What is left at the end of the reply.
    pub fn finish(&mut self) -> Option<String> {
        let rest = std::mem::take(&mut self.pending).trim().to_string();
        (!rest.is_empty()).then_some(rest)
    }

    /// The end of the first complete piece in `pending`.
    fn cut(&self) -> Option<usize> {
        let bytes = self.pending.as_bytes();
        for (i, &c) in bytes.iter().enumerate() {
            let next_space = bytes.get(i + 1).is_some_and(u8::is_ascii_whitespace);
            let long = self.long_clause_marks && i > 60;
            let early_clause = !self.emitted && self.first_clause > 0 && i >= self.first_clause;
            let ends = c == b'\n'
                || (matches!(c, b'.' | b'!' | b'?') && next_space)
                || (long && matches!(c, b':' | b';') && next_space)
                || (early_clause && c == b',' && next_space);
            if !ends {
                continue;
            }
            if self.abbreviation_guard && c == b'.' && i >= 1 {
                let word = self.pending[..i]
                    .rsplit(char::is_whitespace)
                    .next()
                    .unwrap_or("");
                if word.len() <= 2 && word.chars().next().is_some_and(char::is_uppercase) {
                    continue;
                }
            }
            return Some(i + 1);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sentences_split_as_they_complete() {
        let mut s = Sentences::spoken().first_clause(0);
        assert!(s.push("Hello there").unwrap().is_empty());
        assert_eq!(s.push(". How are").unwrap(), vec!["Hello there."]);
        assert_eq!(
            s.push(" you? Dr. Smith said 3.5 is").unwrap(),
            vec!["How are you?"]
        );
        assert!(s.push(" fine").unwrap().is_empty());
        assert_eq!(s.finish().as_deref(), Some("Dr. Smith said 3.5 is fine"));
    }

    #[test]
    fn first_piece_may_end_at_a_comma() {
        let mut s = Sentences::spoken();
        assert_eq!(
            s.push("The tallest mountain on Earth, by height above sea level, is Everest. ")
                .unwrap(),
            vec![
                "The tallest mountain on Earth,",
                "by height above sea level, is Everest."
            ]
        );
    }

    #[test]
    fn long_pieces_end_at_a_colon() {
        let long =
            "There are several things you should remember about the harbor today: the gate moved. ";
        assert_eq!(
            Sentences::spoken().push(long).unwrap(),
            vec![
                "There are several things you should remember about the harbor today:",
                "the gate moved."
            ]
        );
        assert_eq!(Sentences::new().push(long).unwrap().len(), 1);
    }

    #[test]
    fn plain_splitting_has_no_guard() {
        // RAG's configuration: complete sentences only.
        let mut s = Sentences::new().max_bytes(32_000);
        assert!(s.push("The gate is ").unwrap().is_empty());
        assert_eq!(
            s.push("K7 [S1]. Next ").unwrap(),
            vec!["The gate is K7 [S1]."]
        );
        assert_eq!(s.finish().as_deref(), Some("Next"));
        assert_eq!(
            Sentences::new().push("Ask Dr. Smith. ").unwrap(),
            vec!["Ask Dr.", "Smith."]
        );
    }

    #[test]
    fn bounded_configuration() {
        let mut s = Sentences::new().max_bytes(32_000);
        assert!(s.push(&"a".repeat(32_000)).is_ok());
        assert_eq!(s.push("b"), Err(Overflow));
    }
}
