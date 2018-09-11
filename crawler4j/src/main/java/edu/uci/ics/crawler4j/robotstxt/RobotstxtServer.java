package edu.uci.ics.crawler4j.robotstxt;

import edu.uci.ics.crawler4j.url.WebURL;

public interface RobotstxtServer {
    boolean allows(WebURL webURL);
}
