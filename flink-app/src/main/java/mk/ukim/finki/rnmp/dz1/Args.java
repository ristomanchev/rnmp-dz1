package mk.ukim.finki.rnmp.dz1;

/** Reads command-line arguments of the form: --offsets earliest --ooo-sec 180 */
public final class Args {

    private Args() {}

    public static String str(String[] args, String name, String dflt) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(name)) return args[i + 1];
        }
        return dflt;
    }

    public static long num(String[] args, String name, long dflt) {
        return Long.parseLong(str(args, name, String.valueOf(dflt)));
    }

    public static double dbl(String[] args, String name, double dflt) {
        return Double.parseDouble(str(args, name, String.valueOf(dflt)));
    }
}
