package deliver

import (
	"fmt"
	"time"

	"al.essio.dev/pkg/shellescape"
)

// Recipe tells an agent host without a push sink (Claude Code) how to get woken on job: arm its Monitor
// tool on `follow`, or run `wait` in the background. after < 0 means "from now on" (flag omitted).
func Recipe(job string, every time.Duration, match string, after int64) string {
	l := RecipeLines(job, every, match, after)
	return "No T3 thread; to get woken:\n- " + l[0] + "\n- " + l[1]
}

// RecipeLines are Recipe's two options, one per line.
func RecipeLines(job string, every time.Duration, match string, after int64) []string {
	aft, m := "", ""
	if after >= 0 {
		aft = fmt.Sprintf(" --after %d", after)
	}
	if match != "" {
		m = " --match " + shellescape.Quote(match)
	}
	return []string{
		fmt.Sprintf("Arm the Monitor tool with: tail-mode follow %s --every %s%s%s", job, every, m, aft),
		fmt.Sprintf("or run in background: tail-mode wait %s --until RE --timeout 30m%s (exits 0 on match)", job, aft),
	}
}
