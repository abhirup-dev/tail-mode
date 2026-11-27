package deliver

import (
	"fmt"
	"time"

	"al.essio.dev/pkg/shellescape"
)

// Recipe tells an agent host without a push sink (Claude Code) how to get woken on job: arm its Monitor
// tool on `follow`, or run `wait` in the background. after < 0 means "from now on" (flag omitted).
func Recipe(job string, every time.Duration, match string, after int64) string {
	aft, m := "", ""
	if after >= 0 {
		aft = fmt.Sprintf(" --after %d", after)
	}
	if match != "" {
		m = " --match " + shellescape.Quote(match)
	}
	return fmt.Sprintf("No T3 thread; to get woken:\n- Arm the Monitor tool with: tail-mode follow %s --every %s%s%s\n"+
		"- or run in background: tail-mode wait %s --until RE --timeout 30m%s (exits 0 on match)", job, every, m, aft, job, aft)
}
