// Parses each line of standard input as a Gremlin query with the official
// TinkerPop ANTLR grammar (`queryList`) and reports every line that does not
// parse. Exit status 1 when any line fails. Built by
// scripts/validate-gremlin-grammar.sh.

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

public final class ValidateGremlin {
    private static final class Collect extends BaseErrorListener {
        String error;

        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object symbol, int line, int column,
                String message, RecognitionException cause) {
