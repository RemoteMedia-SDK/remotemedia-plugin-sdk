//! A room agent's settings, parsed apart from any model's.

use std::time::Duration;

use serde_json::Value;

/// What the agent publishes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Publish {
    /// `publish: "room"`: the agent link's frames (`presence`, sentence lines).
    Room,
    /// `publish` unset: the `{"kind": "assistant"}` messages of a pipeline
    /// model node; with `sentences_as_text`, each sentence as Text.
    Pipeline { sentences_as_text: bool },
}

#[derive(Clone, Debug, PartialEq)]
pub struct RoomAgentConfig {
    /// What the agent is called.
    pub name: String,
    /// Lowercased words that address it.
    pub wake_words: Vec<String>,
    /// Answer every person's line, named or not.
    pub respond_all: bool,
    /// How long a bare name waits for its question.
    pub follow_up: Duration,
    /// The agent's member id in the room.
    pub member_id: String,
    pub publish: Publish,
}

impl Default for RoomAgentConfig {
    fn default() -> Self {
        Self::from_params(&Value::Null).expect("defaults are valid")
    }
}

impl RoomAgentConfig {
    /// Reads `name`, `wake_words`, `respond`, `follow_up_secs`, `member_id`,
    /// `publish` and `sentences`. String values are taken as they are: expand
    /// `${VAR}` first if the node supports it.
    pub fn from_params(params: &Value) -> Result<Self, String> {
        let s = |key: &str| params[key].as_str().filter(|v| !v.is_empty());
        let name = s("name").unwrap_or("Pixel").to_string();
        let wake_words = match params["wake_words"].as_array() {
            Some(words) => words
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_lowercase)
                .collect(),
            None => vec![name.to_lowercase()],
        };
        let respond_all = match s("respond") {
            None | Some("mention") => false,
            Some("all") => true,
            Some(other) => return Err(format!("respond must be mention or all, not {other:?}")),
        };
        let follow_up_secs = match &params["follow_up_secs"] {
            Value::Null => 8.0,
            v => v
                .as_f64()
                .filter(|v| v.is_finite() && (0.0..=120.0).contains(v))
                .ok_or_else(|| format!("follow_up_secs must be a number from 0 to 120, not {v}"))?,
        };
        let room = match s("publish") {
            None => false,
            Some("room") => true,
            Some(other) => return Err(format!("publish must be room when set, not {other:?}")),
        };
        let sentences_as_text = match s("sentences") {
            None | Some("json") => false,
            Some("text") if room => {
                return Err("sentences: text does not apply to a room agent (publish: room)".into())
            }
            Some("text") => true,
            Some(other) => return Err(format!("sentences must be json or text, not {other:?}")),
        };
        Ok(Self {
            member_id: s("member_id").unwrap_or(&name).to_string(),
            name,
            wake_words,
            respond_all,
            follow_up: Duration::from_secs_f64(follow_up_secs),
            publish: if room {
                Publish::Room
            } else {
                Publish::Pipeline { sentences_as_text }
            },
        })
    }

    pub fn is_room(&self) -> bool {
        self.publish == Publish::Room
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn defaults() {
        let config = RoomAgentConfig::default();
        assert_eq!(config.name, "Pixel");
        assert_eq!(config.wake_words, ["pixel"]);
        assert!(!config.respond_all);
        assert_eq!(config.follow_up, Duration::from_secs(8));
        assert_eq!(config.member_id, "Pixel");
        assert_eq!(
            config.publish,
            Publish::Pipeline {
                sentences_as_text: false
            }
        );
    }

    #[test]
    fn no_model_parameter_needed() {
        let config = RoomAgentConfig::from_params(&json!({
            "name": "Sage", "wake_words": ["Sage", "Assistant"], "respond": "all",
            "follow_up_secs": 2.5, "member_id": "sage-agent", "publish": "room",
        }))
        .unwrap();
        assert_eq!(config.wake_words, ["sage", "assistant"]);
        assert!(config.respond_all && config.is_room());
        assert_eq!(config.follow_up, Duration::from_millis(2500));
        assert_eq!(config.member_id, "sage-agent");
    }

    #[test]
    fn invalid_values_name_their_key() {
        for (params, key) in [
            (json!({"respond": "some"}), "respond"),
            (json!({"publish": "elsewhere"}), "publish"),
            (json!({"sentences": "words"}), "sentences"),
            (json!({"sentences": "text", "publish": "room"}), "sentences"),
            (json!({"follow_up_secs": 121}), "follow_up_secs"),
            (json!({"follow_up_secs": -1}), "follow_up_secs"),
            (json!({"follow_up_secs": "8"}), "follow_up_secs"),
        ] {
            let error = RoomAgentConfig::from_params(&params).unwrap_err();
            assert!(error.contains(key), "{params}: {error}");
        }
        let text = RoomAgentConfig::from_params(&json!({"sentences": "text"})).unwrap();
        assert_eq!(
            text.publish,
            Publish::Pipeline {
                sentences_as_text: true
            }
        );
    }
}
