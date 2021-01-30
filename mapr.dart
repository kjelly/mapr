import 'dart:convert';
import 'dart:io';

import 'package:meta/meta.dart';
import 'package:ansicolor/ansicolor.dart';
import 'package:args/args.dart';
import 'package:dcache/dcache.dart';
import 'package:uuid/uuid.dart';
import 'package:tuple/tuple.dart';
import 'package:yaml/yaml.dart';

Future<ProcessResult> run(String command, {String shell = "sh"}) {
  if (command.startsWith(RegExp('^!'))) {
    var processArguments =
        (jsonDecode(command.substring(1, command.length)) as List<dynamic>)
            .cast<String>();
    return Process.run(processArguments[0], processArguments.sublist(1));
  }
  return Process.run('sh', ['-c', command]);
}

void main(List<String> args) async {
  var parser = new ArgParser();
  parser.addMultiOption('file',
      abbr: "f",
      help: 'Read the content from the files. One line for one loop.',
      valueHelp: 'name=file');
  parser.addMultiOption('command',
      abbr: "c",
      help: 'Read the content from the command. One line for one loop.',
      valueHelp: 'name=command');
  parser.addMultiOption('string',
      abbr: "s",
      help: 'Replace the name with the fixed string',
      valueHelp: 'name=string');
  parser.addMultiOption('tempfile',
      abbr: "t",
      help: 'Replace the name with the tempfile path',
      valueHelp: 'name');
  parser.addMultiOption('uuid',
      abbr: "u", help: 'Replace the name with the uuid', valueHelp: 'name');
  parser.addMultiOption('loop',
      abbr: "l", help: 'generate number', valueHelp: 'name');
  parser.addMultiOption('exception',
      help: 'generate number', valueHelp: 'The command to be run when error.');
  parser.addMultiOption('final',
      help: 'generate number', valueHelp: 'The command to be run when error.');
  parser.addOption('worker', abbr: 'w', defaultsTo: "5");
  parser.addOption('reduce', abbr: 'r', defaultsTo: "");
  parser.addOption('shell', defaultsTo: "sh");
  parser.addOption('stdin', defaultsTo: "");
  parser.addOption('yaml-config', defaultsTo: "");
  parser.addOption('store-stdout',
      defaultsTo: "@stdout", help: "Start from 1. eg: @stdout1.");
  parser.addOption('store-stderr',
      defaultsTo: "@stderr", help: "Start from 1. eg: @stderr1.");
  parser.addOption('store-status',
      defaultsTo: "@status", help: "Start from 1. eg: @stderr1.");

  parser.addFlag('help', abbr: "h", negatable: false);
  parser.addFlag('error',
      defaultsTo: false, help: 'Show the result even if the command failed');
  parser.addFlag('stdout', defaultsTo: true, help: 'Print the stdout');
  parser.addFlag('stderr', defaultsTo: true, help: 'Print the stderr');
  parser.addFlag('header', defaultsTo: true, help: 'Show the header');
  parser.addFlag('print-exception',
      defaultsTo: false, help: 'Show the exception results');
  parser.addFlag('print-final',
      defaultsTo: false, help: 'Show the final results');
  parser.addFlag('last',
      defaultsTo: false, help: 'Show the output of the last command');

  var argResults = parser.parse(args);
  var reduceCommand = argResults['reduce'].toString();
  if (argResults['help']) {
    print("acommand [OPTIONS]... COMMAND [COMMAND]...");
    print(parser.usage);
    return;
  }
  var flags = Map<String, bool>();
  var options = Map<String, String>();
  var multiOptions = Map<String, List<String>>();

  var configMap = YamlMap();
  var configFile = File(argResults['yaml-config']);
  if (configFile.existsSync()) {
    configMap = loadYaml(configFile.readAsStringSync());
  }

  for (var i in MapReduce.flagNames) {
    flags[i] = argResults[i] ?? configMap[i];
  }

  for (var i in MapReduce.optionNames) {
    options[i] = argResults[i] ?? configMap[i];
  }

  for (var i in MapReduce.multiOptionNames) {
    multiOptions[i] = argResults[i] ?? configMap[i];
  }

  print(configMap);

  if ((argResults.rest.length == 0 && reduceCommand.length == 0) &&
      (configMap['run'] == null || configMap['run']?.length == 0)) {
    print("acommand [OPTIONS]... COMMAND [COMMAND]...");
    print(parser.usage);
    return;
  }

  MapReduce(configMap['run']?.cast<String>() ?? argResults.rest,
      flags: flags,
      options: options,
      multiOptions: multiOptions, callback: (String command, ProcessResult p) {
    show(command, p,
        showError: flags['error'],
        stdout: flags['stdout'],
        stderr: flags['stderr'],
        header: flags['header']);
  }).init().then((m) async {
    return m.map();
  }).then((m) {
    return m.reduce();
  }).then((m) {
    m.destroy();
  });
}

void show(String command, ProcessResult p,
    {bool showError = true,
    bool stdout = true,
    bool stderr = true,
    bool header = true}) {
  if (p == null || p?.pid == 0) {
    return;
  }
  var error = '';

  if (p.exitCode != 0) {
    if (!showError) {
      return;
    }
    error = ' with error';
  }
  if (header) {
    print("cmd$error: $command\n");
    if (stdout) {
      print("stdout:\n${p.stdout}\n");
    }
    if (stderr) {
      print("stderr:${p.stderr}");
    }
  } else {
    if (stdout) {
      print("${p.stdout}\n");
    }
    if (stderr) {
      print("${p.stderr}");
    }
  }
}

class Lock {
  int v;
  Lock(this.v);

  void increase() {
    this.v++;
  }

  void decrease() {
    this.v--;
  }

  void wait({microseconds: 1000}) async {
    while (this.v > 0) {
      await Future.delayed(Duration(microseconds: microseconds));
    }
  }
}

Tuple2<String, String> parseKeyValue(String s) {
  var position = s.indexOf('=');
  return Tuple2(s.substring(0, position), s.substring(position + 1, s.length));
}

class Setting {
  String name;
  String value;
  Setting(String s) {
    var position = s.indexOf('=');
    this.name = s.substring(0, position);
    this.value = s.substring(position + 1, s.length);
  }
}

class MapReduce {
  static final multiOptionNames = [
    "file",
    "command",
    "string",
    "tempfile",
    "uuid",
    "loop",
    "exception",
    "final"
  ];
  static final optionNames = [
    "worker",
    "reduce",
    "shell",
    "stdin",
    "store-stdout",
    "store-stderr",
    "store-status"
  ];
  static final flagNames = [
    "error",
    "stdout",
    "stderr",
    "header",
    "print-exception",
    "print-final",
    "last"
  ];
  final List<String> commandList;
  List<Map<String, String>> argList = <Map<String, String>>[];
  Directory tempDir;
  Map<String, bool> flags;
  Map<String, String> options;
  Map<String, List<String>> multiOptions;

  Future<ProcessResult> Function(String) runWrapper;
  Null Function(String, ProcessResult) callback;

  MapReduce(this.commandList,
      {@required this.flags,
      @required this.options,
      @required this.multiOptions,
      this.callback}) {
    this.argList.add(Map<String, String>()); // run least one time
    runWrapper = (String command) {
      return run(command, shell: options['shell']);
    };
  }

  void initFiles() {
    for (var i in multiOptions['file']) {
      var index = 0;
      var parts = i.toString().split('=');
      var name = parts[0];
      var fileName = parts.getRange(1, parts.length).toList().join('=');
      var content = File(fileName).readAsStringSync();
      for (var line in content.split('\n')) {
        line = line.trim();
        if (line.length == 0) {
          continue;
        }
        if (index >= this.argList.length) {
          this.argList.add(Map<String, String>());
        }
        var d = this.argList[index];
        d[name] = line;
        index += 1;
      }
    }
  }

  void initCommands() async {
    for (var i in multiOptions['command']) {
      var index = 0;
      var parts = i.toString().split('=');
      var name = parts[0];
      var command = parts.getRange(1, parts.length).toList().join('=');
      var p = await runWrapper(command);
      for (var line in p.stdout.toString().split('\n')) {
        line = line.trim();
        if (line.length == 0) {
          continue;
        }
        if (index >= this.argList.length) {
          this.argList.add(Map<String, String>());
        }
        var d = this.argList[index];
        d[name] = line;
        index += 1;
      }
    }
  }

  void initStrings() {
    for (var i in multiOptions['string']) {
      var parsedResult = parseKeyValue(i);
      var key = parsedResult.item1;
      var value = parsedResult.item2;
      for (var d in this.argList) {
        d[key] = value;
      }
    }
  }

  void initLoops() {
    for (var i in multiOptions['loop']) {
      var parsedResult = parseKeyValue(i);
      var key = parsedResult.item1;
      var value = parsedResult.item2;
      var position = value.indexOf('-');
      var start = 0;
      if (position == -1) {
        var start = int.tryParse(value) ?? 0;
        for (var d in this.argList) {
          d[key] = start.toString();
          start++;
        }
      } else {
        start = int.tryParse(value.substring(0, position)) ?? 0;
        var end =
            int.tryParse(value.substring(position + 1)) ?? this.argList.length;
        for (var j = start; j < end; j++) {
          var position = j - start;
          if (position >= this.argList.length) {
            this.argList.add(Map<String, String>());
          }
          this.argList[position][key] = j.toString();
        }
      }
    }
  }

  void initTempFiles() {
    var uuid = Uuid();
    for (var i in multiOptions['tempfile']) {
      for (var d in this.argList) {
        var filename = uuid.v4();
        d[i] = "${this.tempDir.path}/$filename";
        File(d[i]).create();
      }
    }
  }

  void initUUID() {
    var uuid = Uuid();
    for (var i in multiOptions['uuid']) {
      for (var d in this.argList) {
        d[i] = uuid.v4();
      }
    }
  }

  void initStdin() {
    var key = options['stdin'];
    var index = 0;
    if (key.length == 0) {
      return;
    }

    while (true) {
      var line = stdin.readLineSync(encoding: Encoding.getByName('utf-8'));
      if (line == null) {
        break;
      }
      line = line.trim();
      if (line.length == 0) {
        continue;
      }
      if (index >= this.argList.length) {
        this.argList.add(Map<String, String>());
      }
      var d = this.argList[index];
      d[key] = line;
      index += 1;
    }
  }

  void initStroe() {
    var stdout = options['store-stdout'].toString();
    var stderr = options['store-stderr'].toString();
    var status = options['store-status'].toString();
    for (var i = 0; i < argList.length; i++) {
      if (stdout.length > 0) {
        this.argList[i][stdout] = '${this.tempDir.path}/$i-stdout';
      }
      if (stderr.length > 0) {
        this.argList[i][stderr] = '${this.tempDir.path}/$i-stderr';
      }
      if (status.length > 0) {
        this.argList[i][status] = '${this.tempDir.path}/$i-status';
      }
    }
  }

  Future<MapReduce> init() async {
    this.tempDir = Directory.systemTemp.createTempSync();

    await this.initCommands();
    this.initFiles();
    this.initLoops();
    this.initStdin();
    this.initTempFiles();
    this.initStrings();
    this.initUUID();
    this.initStroe();
    return this;
  }

  Future<MapReduce> map() async {
    if (this.commandList.length > 0) {
      var worker = Lock(1 - int.tryParse(options['worker']) ?? 5);
      var lock = Lock(this.argList.length);
      var stdout = options['store-stdout'].toString();
      var stderr = options['store-stderr'].toString();
      var status = options['store-status'].toString();

      for (var i in argList) {
        await worker.wait();
        var command = this.commandList[0];
        for (var k in i.keys) {
          command = command.replaceAll(k, i[k]);
        }
        var fp = Future.value(ProcessResult(0, 0, "", ""));
        var oldCommand = null;
        var commandIndex = 0;
        var exception = false;
        worker.increase();
        for (var c in this.commandList.getRange(0, this.commandList.length)) {
          for (var k in i.keys) {
            c = c.replaceAll(k, i[k]);
          }

          var privateOldCommand = oldCommand;
          var privateCommandIndex = commandIndex;
          fp = fp?.then((p) async {
            if (p == null) {
              return null;
            }
            if (p.pid != 0 && !flags['last']) {
              callback(privateOldCommand, p);
            }
            await store(
                p,
                i[stdout] + privateCommandIndex.toString(),
                i[stderr] + privateCommandIndex.toString(),
                i[status] + privateCommandIndex.toString());
            if (p.exitCode == 0) {
              return this.runWrapper(c);
            }
            exception = true;
            return null;
          });
          oldCommand = c;
          commandIndex += 1;
        }
        fp = fp?.then((p) async {
          if (p != null) {
            await store(
                p,
                i[stdout] + commandIndex.toString(),
                i[stderr] + commandIndex.toString(),
                i[status] + commandIndex.toString());
            var privateOldCommand = oldCommand;
            var reduceCommand = options['reduce'].toString();
            if (reduceCommand.length == 0) {
              callback(privateOldCommand, p);
            }
            if (p.exitCode != 0) {
              exception = true;
            }
          }

          var processResultFuture = Future.value(ProcessResult(0, 0, "", ""));

          if (exception) {
            oldCommand = null;
            for (var e in multiOptions['exception']) {
              for (var k in i.keys) {
                e = e.replaceAll(k, i[k]);
              }
              var privateOldCommand = oldCommand;
              processResultFuture = processResultFuture?.then((p) async {
                if (p == null) {
                  return null;
                }
                if (p.pid != 0 && flags['print-exception']) {
                  callback(privateOldCommand, p);
                }
                return this.runWrapper(e);
              });
              oldCommand = e;
            }
          }
          var processResult = await processResultFuture;
          if (processResult != null &&
              processResult.pid != 0 &&
              flags['print-exception']) {
            callback(oldCommand, processResult);
          }

          processResultFuture = Future.value(ProcessResult(0, 0, "", ""));

          for (var e in multiOptions['final']) {
            for (var k in i.keys) {
              e = e.replaceAll(k, i[k]);
            }
            var privateOldCommand = oldCommand;
            processResultFuture = processResultFuture?.then((p) async {
              if (p == null) {
                return null;
              }
              if (p.pid != 0 && flags['print-final']) {
                callback(privateOldCommand, p);
              }
              return this.runWrapper(e);
            });
            oldCommand = e;
          }

          processResult = await processResultFuture;
          if (processResult != null &&
              processResult.pid != 0 &&
              flags['print-final']) {
            callback(oldCommand, processResult);
          }

          worker.decrease();
          lock.decrease();
          return Future.value(ProcessResult(0, 0, "", ""));
        });
      }

      await lock.wait();
    }
    return this;
  }

  Future<MapReduce> reduce() async {
    var reduceCommand = options['reduce'].toString();
    if (reduceCommand.length > 0) {
      for (var i in this.argList[0].keys) {
        if (reduceCommand.contains(i)) {
          var s = '';
          if (i == options['store-stdout'] || i == options['store-stderr']) {
            var re = RegExp(i + '([0-9])+');
            for (var match in re.allMatches(reduceCommand)) {
              var number = match.group(1);
              s = '';
              for (var j = 0; j < this.argList.length; j++) {
                s += this.argList[j][i] + number + ' ';
              }
              reduceCommand = reduceCommand.replaceAll(match.group(0), s);
            }
          } else {
            for (var j = 0; j < this.argList.length; j++) {
              s += argList[j][i] + ' ';
            }
            reduceCommand = reduceCommand.replaceAll(i, s);
          }
        }
      }
      var lock = Lock(1);
      runWrapper(reduceCommand).then((p) {
        show(reduceCommand, p, showError: flags['error']);
        lock.decrease();
      });
      await lock.wait();
    }
    return this;
  }

  void destroy() {
    this.tempDir.delete(recursive: true);
  }

  void store(ProcessResult p, String stdoutFilePath, String stderrFilePath,
      String statusFilePath) async {
    if (p == null || p?.pid == 0) {
      return;
    }
    var lock = Lock(3);

    File(stdoutFilePath).open(mode: FileMode.write).then((f) {
      f.writeStringSync(p.stdout);
      f.closeSync();
      lock.decrease();
    });

    File(stderrFilePath).open(mode: FileMode.write).then((f) {
      f.writeStringSync(p.stderr);
      f.closeSync();
      lock.decrease();
    });

    File(statusFilePath).open(mode: FileMode.write).then((f) {
      f.writeStringSync(p.exitCode.toString());
      f.closeSync();
      lock.decrease();
    });

    await lock.wait();
  }
}
