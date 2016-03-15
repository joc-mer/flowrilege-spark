package exploration;

import java.io.Serializable;

/**
 * Parameters for CSV exploration command.
 *
 * @author joc
 */
public class CSVExploratorParam implements Serializable {

    final String csvFile;
    final long limit;
    final boolean helpNeeded;

    private CSVExploratorParam(String csvFile, long limit, boolean helpNeeded) {
        this.csvFile = csvFile;
        this.limit = limit;
        this.helpNeeded = helpNeeded;
    }

    public static CSVExploratorParam fromArgs(String... args) throws InvalidArgumentException {
        String csvFile = null;
        long limit = 1000L;
        boolean helpNeeded = false;

        for (int argIndex = 0; argIndex < args.length; ++argIndex) {
            switch (args[argIndex]) {
                case "--limit":
                case "-l": {
                    if (args.length - 1 <= argIndex) {
                        throw new InvalidArgumentException("-l need to be followed by an integer");
                    }
                    limit = Long.parseLong(args[argIndex + 1]);
                    break;
                }
                case "--help": 
                case "-h": {
                    helpNeeded = true;
                    break;
                }
                default: {
                    if (argIndex == args.length - 1) {
                        csvFile = args[argIndex];
                    }
                }
            }
        }

        if (csvFile == null) {
            throw new InvalidArgumentException("csv file path should be provided");
        }

        return new CSVExploratorParam(csvFile, limit, helpNeeded);
    }

    public static class InvalidArgumentException extends Exception {

        public InvalidArgumentException() {
        }

        public InvalidArgumentException(String message) {
            super(message);
        }

        public InvalidArgumentException(String message, Throwable cause) {
            super(message, cause);
        }

        public InvalidArgumentException(Throwable cause) {
            super(cause);
        }

        public InvalidArgumentException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
            super(message, cause, enableSuppression, writableStackTrace);
        }

    }

}
