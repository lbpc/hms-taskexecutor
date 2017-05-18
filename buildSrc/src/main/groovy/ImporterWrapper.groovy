import com.linkedin.python.importer.ImporterCLI
import java.util.stream.Collectors

class ImporterWrapper {
    File repo
    Collection<Map<String, Object>> dependencies
    Map<Object, Object> replace

    public void runAction() {
        final List<String> args = new ArrayList<>()
        args.add("--repo")
        args.add(repo.absolutePath)
        final Collection<String> flattenedDependencies =
                dependencies
                        .stream()
                        .map { m -> "${m.get("name")}:${m.get("version")}".toString() }
                        .collect(Collectors.toSet())
        args.addAll(flattenedDependencies)
        if (replace != null) {
            args.add("--replace")
            final String replaceString = replace
                    .entrySet()
                    .stream()
                    .map { e -> "${e.key}=${e.value}".toString() }
                    .collect(Collectors.joining(","))
            args.add(replaceString)
        }
		println "Runing pivy-importer with following args:"
        println args
        ImporterCLI.main(args.toArray(new String[args.size()]))
    }
}
