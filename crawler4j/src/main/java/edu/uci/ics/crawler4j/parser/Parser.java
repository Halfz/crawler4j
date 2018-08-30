package edu.uci.ics.crawler4j.parser;

import edu.uci.ics.crawler4j.crawler.Page;

public interface Parser {

    void parse(Page page, String contextURL);
}
