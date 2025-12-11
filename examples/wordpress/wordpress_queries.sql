-- WordPress 6.9 Installation Queries (MySQL syntax)
-- Test file for Marmot MySQL-to-SQLite transpiler

CREATE DATABASE IF NOT EXISTS wordpress;
USE wordpress;

-- wp_options table (critical - has UNIQUE KEY for ON DUPLICATE KEY UPDATE)
CREATE TABLE wp_options (
	option_id bigint(20) unsigned NOT NULL auto_increment,
	option_name varchar(191) NOT NULL default '',
	option_value longtext NOT NULL,
	autoload varchar(20) NOT NULL default 'yes',
	PRIMARY KEY  (option_id),
	UNIQUE KEY option_name (option_name),
	KEY autoload (autoload)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;

-- wp_users table
CREATE TABLE wp_users (
	ID bigint(20) unsigned NOT NULL auto_increment,
	user_login varchar(60) NOT NULL default '',
	user_pass varchar(255) NOT NULL default '',
	user_nicename varchar(50) NOT NULL default '',
	user_email varchar(100) NOT NULL default '',
	user_url varchar(100) NOT NULL default '',
	user_registered datetime NOT NULL default '0000-00-00 00:00:00',
	user_activation_key varchar(255) NOT NULL default '',
	user_status int(11) NOT NULL default '0',
	display_name varchar(250) NOT NULL default '',
	PRIMARY KEY  (ID),
	KEY user_login_key (user_login),
	KEY user_nicename (user_nicename),
	KEY user_email (user_email)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;

-- wp_usermeta table
CREATE TABLE wp_usermeta (
	umeta_id bigint(20) unsigned NOT NULL auto_increment,
	user_id bigint(20) unsigned NOT NULL default '0',
	meta_key varchar(255) default NULL,
	meta_value longtext,
	PRIMARY KEY  (umeta_id),
	KEY user_id (user_id),
	KEY meta_key (meta_key(191))
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;

-- wp_posts table
CREATE TABLE wp_posts (
	ID bigint(20) unsigned NOT NULL auto_increment,
	post_author bigint(20) unsigned NOT NULL default '0',
	post_date datetime NOT NULL default '0000-00-00 00:00:00',
	post_date_gmt datetime NOT NULL default '0000-00-00 00:00:00',
	post_content longtext NOT NULL,
	post_title text NOT NULL,
	post_excerpt text NOT NULL,
	post_status varchar(20) NOT NULL default 'publish',
	comment_status varchar(20) NOT NULL default 'open',
	ping_status varchar(20) NOT NULL default 'open',
	post_password varchar(255) NOT NULL default '',
	post_name varchar(200) NOT NULL default '',
	to_ping text NOT NULL,
	pinged text NOT NULL,
	post_modified datetime NOT NULL default '0000-00-00 00:00:00',
	post_modified_gmt datetime NOT NULL default '0000-00-00 00:00:00',
	post_content_filtered longtext NOT NULL,
	post_parent bigint(20) unsigned NOT NULL default '0',
	guid varchar(255) NOT NULL default '',
	menu_order int(11) NOT NULL default '0',
	post_type varchar(20) NOT NULL default 'post',
	post_mime_type varchar(100) NOT NULL default '',
	comment_count bigint(20) NOT NULL default '0',
	PRIMARY KEY  (ID),
	KEY post_name (post_name(191)),
	KEY type_status_date (post_type,post_status,post_date,ID),
	KEY post_parent (post_parent),
	KEY post_author (post_author)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;

-- wp_postmeta table
CREATE TABLE wp_postmeta (
	meta_id bigint(20) unsigned NOT NULL auto_increment,
	post_id bigint(20) unsigned NOT NULL default '0',
	meta_key varchar(255) default NULL,
	meta_value longtext,
	PRIMARY KEY  (meta_id),
	KEY post_id (post_id),
	KEY meta_key (meta_key(191))
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;

-- wp_comments table
CREATE TABLE wp_comments (
	comment_ID bigint(20) unsigned NOT NULL auto_increment,
	comment_post_ID bigint(20) unsigned NOT NULL default '0',
	comment_author tinytext NOT NULL,
	comment_author_email varchar(100) NOT NULL default '',
	comment_author_url varchar(200) NOT NULL default '',
	comment_author_IP varchar(100) NOT NULL default '',
	comment_date datetime NOT NULL default '0000-00-00 00:00:00',
	comment_date_gmt datetime NOT NULL default '0000-00-00 00:00:00',
	comment_content text NOT NULL,
	comment_karma int(11) NOT NULL default '0',
	comment_approved varchar(20) NOT NULL default '1',
	comment_agent varchar(255) NOT NULL default '',
	comment_type varchar(20) NOT NULL default 'comment',
	comment_parent bigint(20) unsigned NOT NULL default '0',
	user_id bigint(20) unsigned NOT NULL default '0',
	PRIMARY KEY  (comment_ID),
	KEY comment_post_ID (comment_post_ID),
	KEY comment_approved_date_gmt (comment_approved,comment_date_gmt),
	KEY comment_date_gmt (comment_date_gmt),
	KEY comment_parent (comment_parent)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;

-- wp_terms table
CREATE TABLE wp_terms (
	term_id bigint(20) unsigned NOT NULL auto_increment,
	name varchar(200) NOT NULL default '',
	slug varchar(200) NOT NULL default '',
	term_group bigint(10) NOT NULL default 0,
	PRIMARY KEY  (term_id),
	KEY slug (slug(191)),
	KEY name (name(191))
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;

-- wp_term_taxonomy table
CREATE TABLE wp_term_taxonomy (
	term_taxonomy_id bigint(20) unsigned NOT NULL auto_increment,
	term_id bigint(20) unsigned NOT NULL default 0,
	taxonomy varchar(32) NOT NULL default '',
	description longtext NOT NULL,
	parent bigint(20) unsigned NOT NULL default 0,
	count bigint(20) NOT NULL default 0,
	PRIMARY KEY  (term_taxonomy_id),
	UNIQUE KEY term_id_taxonomy (term_id,taxonomy),
	KEY taxonomy (taxonomy)
) DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;

-- Test INSERT ON DUPLICATE KEY UPDATE
INSERT INTO wp_options (option_name, option_value, autoload) VALUES ('siteurl', 'http://localhost:8080', 'yes') ON DUPLICATE KEY UPDATE option_value = VALUES(option_value);
INSERT INTO wp_options (option_name, option_value, autoload) VALUES ('home', 'http://localhost:8080', 'yes') ON DUPLICATE KEY UPDATE option_value = VALUES(option_value);
INSERT INTO wp_options (option_name, option_value, autoload) VALUES ('blogname', 'Test Blog', 'yes') ON DUPLICATE KEY UPDATE option_value = VALUES(option_value);

-- Update existing option (should update, not insert)
INSERT INTO wp_options (option_name, option_value, autoload) VALUES ('siteurl', 'http://localhost:9000', 'yes') ON DUPLICATE KEY UPDATE option_value = VALUES(option_value);

-- Verify results
SELECT option_name, option_value FROM wp_options ORDER BY option_name;
