package logger

import (
	"os"
	"strings"
	"time"

	"github.com/natefinch/lumberjack"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	logTmFmt     = "2006-01-02 15:04:05.000"
	callerLenMax = 33
)

var logger *zap.Logger

func InitLogger(logpath string, isDebug bool) {
	// 日志分割
	hook := lumberjack.Logger{
		Filename:   logpath, // 日志文件路径，默认 os.TempDir()
		MaxSize:    10,      // 每个日志文件保存M
		MaxBackups: 30,      // 保留30个备份，默认不限
		MaxAge:     7,       // 保留7天，默认不限
		Compress:   false,   // 是否压缩，默认不压缩
	}
	write := zapcore.AddSync(&hook)

	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    cEncodeLevel,
		EncodeTime:     cEncodeTime,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   cEncodeCaller,
		EncodeName:     zapcore.FullNameEncoder,
	}
	// 设置日志级别5
	atomicLevel := zap.NewAtomicLevel()
	var level zapcore.Level
	var core zapcore.Core
	if isDebug {
		level = zap.DebugLevel
		atomicLevel.SetLevel(level)
		core = zapcore.NewCore(
			zapcore.NewConsoleEncoder(encoderConfig),
			zapcore.NewMultiWriteSyncer(zapcore.AddSync(os.Stdout), zapcore.AddSync(write)), // 打印到控制台和文件
			level,
		)
	} else {
		level = zap.InfoLevel
		atomicLevel.SetLevel(level)
		core = zapcore.NewCore(
			zapcore.NewJSONEncoder(encoderConfig),
			zapcore.NewMultiWriteSyncer(zapcore.AddSync(write)), // 只打印到文件
			level,
		)
	}

	// 开启开发模式，堆栈跟踪
	caller := zap.AddCaller()
	callerSkip := zap.AddCallerSkip(1)
	// 开启文件及行号
	development := zap.Development()
	// 设置初始化字段,如：添加一个服务器名称
	// filed := zap.Fields(zap.String("serviceName", "serviceName"))
	// 构造日志
	logger = zap.New(core, caller, callerSkip, development)
	logger.Info("DefaultLogger init success")
}

// cEncodeLevel 自定义日志级别显示
func cEncodeLevel(level zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString("[" + level.CapitalString() + "]")
}

// cEncodeTime 自定义时间格式显示
func cEncodeTime(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.Format(logTmFmt))
}

// cEncodeCaller 自定义行号显示
func cEncodeCaller(caller zapcore.EntryCaller, enc zapcore.PrimitiveArrayEncoder) {
	path := caller.TrimmedPath()
	len := len(path)
	spaceStr := ""
	if len < callerLenMax {
		spaceStr = strings.Repeat(" ", callerLenMax-len)
	}
	enc.AppendString("[" + path + "]" + spaceStr)
}

func Debug(msg string, args ...interface{}) {
	logger.Sugar().Debug(msg, args)
}

func Info(msg string, args ...interface{}) {
	logger.Sugar().Info(msg, args)
}

func Error(msg string, args ...interface{}) {
	logger.Sugar().Error(msg, args)
}

func PDebug(sid uint32, msg string, args ...interface{}) {
	logger.Debug(msg,
		zap.Uint32("sid", sid),
	)
}
