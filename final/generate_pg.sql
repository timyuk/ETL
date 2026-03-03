DROP TABLE IF EXISTS user_session_action;
DROP TABLE IF EXISTS user_session_page;
DROP TABLE IF EXISTS user_session;
DROP TABLE IF EXISTS event_log;
DROP TABLE IF EXISTS support_ticket_message;
DROP TABLE IF EXISTS support_ticket;
DROP TABLE IF EXISTS user_recommendation;
DROP TABLE IF EXISTS moderation_queue;


CREATE TABLE user_session (
    session_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    device TEXT
);

CREATE TABLE user_session_page (
    id SERIAL PRIMARY KEY,
    session_id TEXT REFERENCES user_session(session_id),
    page TEXT NOT NULL,
    visit_order INT NOT NULL
);

ALTER TABLE user_session_page
ADD CONSTRAINT unique_user_session_page UNIQUE (session_id, visit_order);


CREATE TABLE user_session_action (
    id SERIAL PRIMARY KEY,
    session_id TEXT REFERENCES user_session(session_id),
    action TEXT NOT NULL,
    action_order INT NOT NULL
);

ALTER TABLE user_session_action
ADD CONSTRAINT unique_user_session_action UNIQUE (session_id, action_order);

CREATE TABLE event_log (
    event_id TEXT PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    event_type TEXT NOT NULL,
    details TEXT NOT NULL
);

CREATE TABLE support_ticket (
    ticket_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    status TEXT NOT NULL,
    issue_type TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE support_ticket_message (
    id SERIAL PRIMARY KEY,
    ticket_id TEXT REFERENCES support_ticket(ticket_id),
    sender TEXT NOT NULL,
    message TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL
);

ALTER TABLE support_ticket_message
ADD CONSTRAINT unique_support_ticket_message UNIQUE (ticket_id, sender, message, timestamp);

CREATE TABLE user_recommendation (
    id SERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    recommended_product TEXT NOT NULL,
    last_updated TIMESTAMPTZ NOT NULL
);

ALTER TABLE user_recommendation
ADD CONSTRAINT unique_user_recommendation UNIQUE (user_id, recommended_product);

CREATE TABLE moderation_queue (
    review_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    review_text TEXT NOT NULL,
    rating INT NOT NULL,
    moderation_status TEXT NOT NULL,
    flags TEXT[],
    submitted_at TIMESTAMPTZ NOT NULL
);