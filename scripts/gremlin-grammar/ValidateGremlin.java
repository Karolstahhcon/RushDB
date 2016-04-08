// Parses each line of standard input as a Gremlin query with the official
// TinkerPop ANTLR grammar (`queryList`) and reports every line that does not
// parse. Exit status 1 when any line fails. Built by
// scripts/validate-gremlin-grammar.sh.

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
