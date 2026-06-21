import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

public class Main {

    private static Path currentDirectory = Paths.get(System.getProperty("user.dir")).toAbsolutePath();
    private static final List<BackgroundJob> backgroundJobs = new ArrayList<>();
    private static final List<String> BUILTINS = List.of("echo", "type", "exit", "pwd", "cd", "jobs");

    static class BackgroundJob {
        int jobNumber;
        long pid;
        String baseCommand; 
        String status;
        List<Thread> pipelineThreads;
        List<Process> pipelineProcesses;

        public BackgroundJob(int jobNumber, long pid, String baseCommand, String status, List<Thread> pipelineThreads, List<Process> pipelineProcesses) {
            this.jobNumber = jobNumber;
            this.pid = pid;
            this.baseCommand = baseCommand;
            this.status = status;
            this.pipelineThreads = pipelineThreads;
            this.pipelineProcesses = pipelineProcesses;
        }

        public boolean isAlive() {
            for (Process p : pipelineProcesses) {
                if (p.isAlive()) return true;
            }
            for (Thread t : pipelineThreads) {
                if (t.isAlive()) return true;
            }
            return false;
        }
    }

    public static void main(String[] args) throws Exception {
        Scanner sc = new Scanner(System.in);

        while (true) {
            reapCompletedJobs(System.out, true);

            System.out.print("$ ");
            if (!sc.hasNextLine()) break;
            String input = sc.nextLine().trim();

            if (input.isEmpty()) continue;

            List<String> tokens = parseArguments(input);
            if (tokens.isEmpty()) continue;

            boolean isBackground = false;
            if (tokens.get(tokens.size() - 1).equals("&")) {
                isBackground = true;
                tokens.remove(tokens.size() - 1);
            }

            if (tokens.isEmpty()) continue;

            // --- Check for Pipelines ---
            if (tokens.contains("|")) {
                handlePipeline(tokens, isBackground);
                continue;
            }

            // Standard Builtin/External command handling (Non-piped execution)
            executeSingleCommand(tokens, isBackground, null, null);
        }
    }

    // --- Core Pipeline Handling with Builtin Support ---
    private static void handlePipeline(List<String> tokens, boolean isBackground) {
        List<List<String>> commandsTokens = new ArrayList<>();
        List<String> currentCmd = new ArrayList<>();

        for (String token : tokens) {
            if (token.equals("|")) {
                if (!currentCmd.isEmpty()) {
                    commandsTokens.add(currentCmd);
                    currentCmd = new ArrayList<>();
                }
            } else {
                currentCmd.add(token);
            }
        }
        if (!currentCmd.isEmpty()) {
            commandsTokens.add(currentCmd);
        }

        int numCmds = commandsTokens.size();
        List<Process> processes = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();

        // Create inter-process communication pipes
        PipedStreamPair[] pipes = new PipedStreamPair[numCmds - 1];
        for (int i = 0; i < numCmds - 1; i++) {
            try {
                pipes[i] = new PipedStreamPair();
            } catch (IOException e) {
                System.out.println("Pipe creation failed: " + e.getMessage());
                return;
            }
        }

        // Spawn each stage of the pipeline
        for (int i = 0; i < numCmds; i++) {
            List<String> cmdArgs = commandsTokens.get(i);
            String command = cmdArgs.get(0);

            InputStream stageIn = (i == 0) ? System.in : pipes[i - 1].in;
            OutputStream stageOut = (i == numCmds - 1) ? System.out : pipes[i].out;

            if (BUILTINS.contains(command)) {
                final int stageIdx = i;
                Thread builtinThread = new Thread(() -> {
                    try {
                        executeBuiltin(cmdArgs, stageIn, stageOut, System.err);
                    } finally {
                        if (stageIdx < numCmds - 1) {
                            try { stageOut.close(); } catch (IOException ignored) {}
                        }
                    }
                });
                threads.add(builtinThread);
                builtinThread.start();
            } else {
                String fullPath = getExecutablePath(command);
                if (fullPath == null) {
                    System.out.println(command + ": command not found");
                    return;
                }
                
                // FIX: Keep short name in cmdArgs to preserve expected argv[0] layout structure
                try {
                    ProcessBuilder pb = new ProcessBuilder(cmdArgs);
                    pb.directory(currentDirectory.toFile());
                    pb.environment().put("PATH", System.getenv("PATH"));

                    if (i == 0) {
                        pb.redirectInput(ProcessBuilder.Redirect.INHERIT);
                    } else {
                        pb.redirectInput(ProcessBuilder.Redirect.PIPE);
                    }

                    if (i == numCmds - 1) {
                        pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
                    } else {
                        pb.redirectOutput(ProcessBuilder.Redirect.PIPE);
                    }
                    pb.redirectError(ProcessBuilder.Redirect.INHERIT);

                    Process proc = pb.start();
                    processes.add(proc);
                    
                    if (i > 0 && stageIn instanceof java.io.PipedInputStream) {
                        bridgeStreamsAsync(stageIn, proc.getOutputStream());
                    }
                    if (i < numCmds - 1 && stageOut instanceof java.io.PipedOutputStream) {
                        bridgeStreamsAsync(proc.getInputStream(), stageOut);
                    }

                } catch (IOException e) {
                    System.out.println("Error starting pipeline process: " + e.getMessage());
                }
            }
        }

        if (isBackground) {
            int assignedJobId = 1;
            if (!backgroundJobs.isEmpty()) {
                int maxJobNum = 0;
                for (BackgroundJob job : backgroundJobs) {
                    if (job.jobNumber > maxJobNum) maxJobNum = job.jobNumber;
                }
                assignedJobId = maxJobNum + 1;
            }
            long repPid = processes.isEmpty() ? Thread.currentThread().getId() : processes.get(processes.size() - 1).pid();
            System.out.println("[" + assignedJobId + "] " + repPid);
            backgroundJobs.add(new BackgroundJob(assignedJobId, repPid, String.join(" ", tokens), "Running", threads, processes));
        } else {
            for (Process p : processes) {
                try { p.waitFor(); } catch (InterruptedException ignored) {}
            }
            for (Thread t : threads) {
                try { t.join(); } catch (InterruptedException ignored) {}
            }
        }
    }

    private static void bridgeStreamsAsync(InputStream in, OutputStream out) {
        Thread t = new Thread(() -> {
            byte[] buffer = new byte[4096];
            int bytesRead;
            try {
                while ((bytesRead = in.read(buffer)) != -1) {
                    out.write(buffer, 0, bytesRead);
                    out.flush();
                }
            } catch (IOException ignored) {
            } finally {
                try { out.close(); } catch (IOException ignored) {}
            }
        });
        t.setDaemon(true);
        t.start();
    }

    private static class PipedStreamPair {
        final java.io.PipedInputStream in;
        final java.io.PipedOutputStream out;

        PipedStreamPair() throws IOException {
            this.out = new java.io.PipedOutputStream();
            this.in = new java.io.PipedInputStream(this.out, 65536);
        }
    }

    private static void executeBuiltin(List<String> cmdTokens, InputStream in, OutputStream out, OutputStream err) {
        PrintStream writer = new PrintStream(out, true);
        String command = cmdTokens.get(0);

        if (command.equals("echo")) {
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i < cmdTokens.size(); i++) {
                sb.append(cmdTokens.get(i));
                if (i < cmdTokens.size() - 1) sb.append(" ");
            }
            writer.println(sb.toString());
        } else if (command.equals("type")) {
            if (cmdTokens.size() < 2) return;
            String checkStr = cmdTokens.get(1);
            if (BUILTINS.contains(checkStr)) {
                writer.println(checkStr + " is a shell builtin");
            } else {
                String executablePath = getExecutablePath(checkStr);
                writer.println((executablePath != null) ? checkStr + " is " + executablePath : checkStr + ": not found");
            }
        } else if (command.equals("pwd")) {
            writer.println(currentDirectory.toString());
        } else if (command.equals("jobs")) {
            reapCompletedJobs(writer, false);
        } else if (command.equals("exit")) {
            System.exit(0);
        }
    }

    private static void executeSingleCommand(List<String> tokens, boolean isBackground, InputStream customIn, OutputStream customOut) {
        String redirectFile = null;
        String redirectErrFile = null;
        boolean appendStdout = false;
        boolean appendStderr = false;
        int redirectIndex = -1;

        for (int i = 0; i < tokens.size(); i++) {
            String token = tokens.get(i);
            if (token.equals(">>") || token.equals("1>>")) {
                if (i + 1 < tokens.size()) { redirectFile = tokens.get(i + 1); appendStdout = true; redirectIndex = i; break; }
            } else if (token.equals(">") || token.equals("1>")) {
                if (i + 1 < tokens.size()) { redirectFile = tokens.get(i + 1); appendStdout = false; redirectIndex = i; break; }
            } else if (token.equals("2>>")) {
                if (i + 1 < tokens.size()) { redirectErrFile = tokens.get(i + 1); appendStderr = true; redirectIndex = i; break; }
            } else if (token.equals("2>")) {
                if (i + 1 < tokens.size()) { redirectErrFile = tokens.get(i + 1); appendStderr = false; redirectIndex = i; break; }
            }
        }

        List<String> cmdTokens = (redirectIndex != -1) ? new ArrayList<>(tokens.subList(0, redirectIndex)) : tokens;
        if (cmdTokens.isEmpty()) return;

        String command = cmdTokens.get(0);

        OutputStream stdoutTarget = (customOut != null) ? customOut : System.out;
        if (redirectFile != null) {
            try {
                File f = new File(redirectFile);
                if (f.getParentFile() != null) f.getParentFile().mkdirs();
                stdoutTarget = new FileOutputStream(f, appendStdout);
            } catch (IOException ignored) {}
        }

        OutputStream stderrTarget = System.err;
        if (redirectErrFile != null) {
            try {
                File f = new File(redirectErrFile);
                if (f.getParentFile() != null) f.getParentFile().mkdirs();
                stderrTarget = new FileOutputStream(f, appendStderr);
            } catch (IOException ignored) {}
        }

        if (BUILTINS.contains(command)) {
            if (command.equals("cd")) {
                if (cmdTokens.size() < 2) return;
                String targetPathStr = cmdTokens.get(1);
                Path targetPath = targetPathStr.equals("~") ? Paths.get(System.getenv("HOME")) :
                                   targetPathStr.startsWith("~/") ? Paths.get(System.getenv("HOME")).resolve(targetPathStr.substring(2)) :
                                   targetPathStr.startsWith("/") ? Paths.get(targetPathStr) : currentDirectory.resolve(targetPathStr);
                targetPath = targetPath.toAbsolutePath().normalize();
                if (targetPath.toFile().exists() && targetPath.toFile().isDirectory()) {
                    currentDirectory = targetPath;
                } else {
                    PrintStream errWriter = new PrintStream(stderrTarget, true);
                    errWriter.println("cd: " + targetPathStr + ": No such file or directory");
                }
            } else {
                executeBuiltin(cmdTokens, (customIn != null ? customIn : System.in), stdoutTarget, stderrTarget);
            }
        } else {
            String executablePath = getExecutablePath(command);
            if (executablePath != null) {
                try {
                    String baseCommandStr = String.join(" ", tokens);

                    // FIX: DO NOT replace command name with absolute path in cmdTokens array
                    ProcessBuilder pb = new ProcessBuilder(cmdTokens);
                    pb.directory(currentDirectory.toFile());
                    pb.environment().put("PATH", System.getenv("PATH"));
                    
                    if (redirectFile != null) {
                        pb.redirectOutput(appendStdout ? ProcessBuilder.Redirect.appendTo(new File(redirectFile)) : ProcessBuilder.Redirect.to(new File(redirectFile)));
                    } else {
                        pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
                    }
                    
                    if (redirectErrFile != null) {
                        pb.redirectError(appendStderr ? ProcessBuilder.Redirect.appendTo(new File(redirectErrFile)) : ProcessBuilder.Redirect.to(new File(redirectErrFile)));
                    } else {
                        pb.redirectError(ProcessBuilder.Redirect.INHERIT);
                    }

                    Process process = pb.start();

                    if (isBackground) {
                        int assignedJobId = 1;
                        if (!backgroundJobs.isEmpty()) {
                            int maxJobNum = 0;
                            for (BackgroundJob job : backgroundJobs) {
                                if (job.jobNumber > maxJobNum) maxJobNum = job.jobNumber;
                            }
                            assignedJobId = maxJobNum + 1;
                        }
                        System.out.println("[" + assignedJobId + "] " + process.pid());
                        backgroundJobs.add(new BackgroundJob(assignedJobId, process.pid(), baseCommandStr, "Running", List.of(), List.of(process)));
                    } else {
                        process.waitFor();
                    }
                } catch (IOException | InterruptedException e) {
                    System.out.println("Error executing command : " + e.getMessage());
                }
            } else {
                System.out.println(command + ": command not found");
            }
        }
    }

    private static void reapCompletedJobs(PrintStream targetStream, boolean onlyPrintDone) {
        for (BackgroundJob job : backgroundJobs) {
            if (job.status.equals("Running") && !job.isAlive()) {
                job.status = "Done";
            }
        }

        StringBuilder output = new StringBuilder();
        int totalJobs = backgroundJobs.size();

        for (int i = 0; i < totalJobs; i++) {
            BackgroundJob job = backgroundJobs.get(i);
            if (onlyPrintDone && !job.status.equals("Done")) continue;

            char marker = ' ';
            if (i == totalJobs - 1) marker = '+';
            else if (i == totalJobs - 2) marker = '-';

            String displayCommand = job.status.equals("Done") ? job.baseCommand : job.baseCommand + " &";
            output.append(String.format("[%d]%c  %-24s%s\n", job.jobNumber, marker, job.status, displayCommand));
        }

        backgroundJobs.removeIf(job -> job.status.equals("Done"));

        if (output.length() > 0) {
            targetStream.print(output.toString());
        }
    }

    private static List<String> parseArguments(String input) {
        List<String> tokens = new ArrayList<>();
        StringBuilder currentToken = new StringBuilder();
        boolean insideSingleQuotes = false;
        boolean insideDoubleQuotes = false;
        boolean tokenStarted = false;

        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            if (insideSingleQuotes) {
                if (c == '\'') insideSingleQuotes = false;
                else currentToken.append(c);
                tokenStarted = true;
            } else if (insideDoubleQuotes) {
                if (c == '\\') {
                    if (i + 1 < input.length()) {
                        char nextChar = input.charAt(i + 1);
                        if (nextChar == '"' || nextChar == '\\') {
                            currentToken.append(nextChar); i++;
                        } else {
                            currentToken.append(c);
                        }
                    } else {
                        currentToken.append(c);
                    }
                } else if (c == '"') {
                    insideDoubleQuotes = false;
                } else {
                    currentToken.append(c);
                }
                tokenStarted = true;
            } else {
                if (c == '\\') {
                    if (i + 1 < input.length()) {
                        i++; currentToken.append(input.charAt(i)); tokenStarted = true;
                    }
                } else if (c == '\'') {
                    insideSingleQuotes = true; tokenStarted = true;
                } else if (c == '"') {
                    insideDoubleQuotes = true; tokenStarted = true;
                } else if (Character.isWhitespace(c)) {
                    if (tokenStarted) {
                        tokens.add(currentToken.toString());
                        currentToken.setLength(0);
                        tokenStarted = false;
                    }
                } else {
                    currentToken.append(c); tokenStarted = true;
                }
            }
        }
        if (tokenStarted) tokens.add(currentToken.toString());
        return tokens;
    }

    private static String getExecutablePath(String command) {
        String pathEnv = System.getenv("PATH");
        if (pathEnv != null) {
            String[] directories = pathEnv.split(File.pathSeparator);
            for (String dir : directories) {
                Path fullPath = Paths.get(dir, command);
                if (Files.exists(fullPath) && Files.isExecutable(fullPath)) {
                    return fullPath.toAbsolutePath().toString();
                }
            }
        }
        return null;
    }
}