package graphvent

import (
  "fmt"
  "github.com/rs/zerolog"
  "os"
  "sync"
  "encoding/json"
)

// A Logger is passed around to record events happening to components enabled by SetComponents
type Logger interface {
  SetComponents(components []string) error
  // Log a formatted string
  Logf(component string, format string, items ... interface{})
  // Log a map of attributes and a format string
  Logm(component string, fields map[string]interface{}, format string, items ... interface{})
  // Log a structure to a file by marshalling and unmarshalling the json
  Logj(component string, s interface{}, format string, items ... interface{})
}

func NewConsoleLogger(components []string) *ConsoleLogger {
  logger := &ConsoleLogger{
    loggers: map[string]zerolog.Logger{},
    components: []string{},
  }

  logger.SetComponents(components)

  return logger
}

// A ConsoleLogger logs to stdout
type ConsoleLogger struct {
  loggers map[string]zerolog.Logger
  components_lock sync.Mutex
  components []string
}

func (logger * ConsoleLogger) SetComponents(components []string) error {
  logger.components_lock.Lock()
  defer logger.components_lock.Unlock()

  component_enabled := func (component string) bool {
    for _, c := range(components) {
      if c == component {
        return true
      }
    }
    return false
  }

  for c, _ := range(logger.loggers) {
    if component_enabled(c) == false {
      delete(logger.loggers, c)
    }
  }

  for _, c := range(components) {
    _, exists := logger.loggers[c]
    if component_enabled(c) == true && exists == false {
      logger.loggers[c] = zerolog.New(os.Stdout).With().Timestamp().Str("component", c).Logger()
    }
  }
  return nil
}

func (logger * ConsoleLogger) Logm(component string, fields map[string]interface{}, format string, items ... interface{}) {
  l, exists := logger.loggers[component]
  if exists == true {
    log := l.Log()
    for key, value := range(fields) {
      log = log.Str(key, fmt.Sprintf("%+v", value))
    }
    log.Msg(fmt.Sprintf(format, items...))
  }
}

func (logger * ConsoleLogger) Logf(component string, format string, items ... interface{}) {
  l, exists := logger.loggers[component]
  if exists == true {
    l.Log().Msg(fmt.Sprintf(format, items...))
  }
}

func (logger * ConsoleLogger) Logj(component string, s interface{}, format string, items ... interface{}) {
  m := map[string]interface{}{}
  ser, err := json.Marshal(s)
  if err != nil {
    panic("LOG_MARSHAL_ERR")
  }
  err = json.Unmarshal(ser, &m)
  if err != nil {
    panic("LOG_UNMARSHAL_ERR")
  }
  logger.Logm(component, m, format, items...)
}
