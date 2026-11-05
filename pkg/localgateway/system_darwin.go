//go:build darwin

package localgateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
)

type fileImage struct {
	Exists   bool
	Data     []byte
	Mode     uint32
	UID, GID int
}
type resolverImage struct {
	Before  fileImage
	Written []byte
}
type hostMapping struct{ IP, Name string }
type systemJournal struct {
	Hosts         fileImage
	ExpectedHosts []byte
	ExactHosts    bool
	Removed       []hostMapping
	Resolvers     map[string]resolverImage
	OwnedIPs      []string
	PendingIPs    []string
}

// SystemState owns a private recovery journal and only the resolver contents and
// stale loopback hostnames changed during this interactive gateway session.
type SystemState struct {
	dir, hosts, resolvers string
	lock                  *os.File
	journal               systemJournal
}

func NewSystemState(dir string) (*SystemState, error) {
	return newSystemState(dir, "/etc/hosts", "/etc/resolver", false)
}

func RecoverSystemState(dir string) (*SystemState, error) {
	return newSystemState(dir, "/etc/hosts", "/etc/resolver", true)
}

func newSystemState(dir, hosts, resolvers string, recovering bool) (*SystemState, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return nil, fmt.Errorf("state directory must be a private directory: %s", dir)
	}
	lock, err := os.OpenFile(filepath.Join(dir, "lock"), os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0600)
	if err != nil {
		return nil, err
	}
	if info, statErr := lock.Stat(); statErr != nil || !info.Mode().IsRegular() {
		lock.Close()
		return nil, errors.New("state lock must be a regular file")
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("another gateway owns state directory: %w", err)
	}
	s := &SystemState{dir: dir, hosts: hosts, resolvers: resolvers, lock: lock}
	fail := func(err error) (*SystemState, error) { s.Close(); return nil, err }
	journalPath := filepath.Join(dir, "journal.json")
	if recovering {
		image, err := readImage(journalPath)
		if err != nil {
			return fail(err)
		}
		if !image.Exists {
			return fail(errors.New("no recovery journal exists"))
		}
		if image.UID != os.Geteuid() {
			return fail(errors.New("recovery journal is not owned by this user"))
		}
		if err = json.Unmarshal(image.Data, &s.journal); err != nil {
			return fail(err)
		}
		return s, nil
	}
	if _, err = os.Lstat(journalPath); err == nil {
		return fail(errors.New("unclean session journal exists; run --cleanup with this state directory first"))
	} else if !errors.Is(err, os.ErrNotExist) {
		return fail(err)
	}
	original, err := readImage(hosts)
	if err != nil {
		return fail(err)
	}
	if !original.Exists {
		return fail(errors.New("hosts file does not exist"))
	}
	s.journal = systemJournal{Hosts: original, ExpectedHosts: original.Data, ExactHosts: true, Resolvers: map[string]resolverImage{}}
	if err = s.save(); err != nil {
		return fail(err)
	}
	return s, nil
}

func (s *SystemState) save() error {
	data, err := json.MarshalIndent(s.journal, "", "  ")
	if err != nil {
		return err
	}
	return replaceImage(filepath.Join(s.dir, "journal.json"), fileImage{Exists: true, Data: append(data, '\n'), Mode: 0600, UID: os.Geteuid(), GID: os.Getegid()})
}

func (s *SystemState) PendingIP(ip string) error {
	s.journal.PendingIPs = append(s.journal.PendingIPs, ip)
	return s.save()
}

// Aliases accepts only addresses whose acquisition was confirmed successful.
// Unconfirmed partial attempts must remain Pending for manual inspection.
func (s *SystemState) Aliases(owned []string) error {
	s.journal.OwnedIPs = append([]string(nil), owned...)
	pending := s.journal.PendingIPs[:0]
	for _, ip := range s.journal.PendingIPs {
		found := false
		for _, own := range owned {
			if ip == own {
				found = true
				break
			}
		}
		if !found {
			pending = append(pending, ip)
		}
	}
	s.journal.PendingIPs = pending
	return s.save()
}
func (s *SystemState) RecordedAliases() ([]string, []string) {
	return append([]string(nil), s.journal.OwnedIPs...), append([]string(nil), s.journal.PendingIPs...)
}

func (s *SystemState) Configure(domains []string, port int, hostnames []string) error {
	if port < 1 || port > 65535 {
		return errors.New("invalid local DNS port")
	}
	for _, domain := range domains {
		if !validDomain(domain) {
			return fmt.Errorf("invalid resolver domain %q", domain)
		}
	}
	if err := os.MkdirAll(s.resolvers, 0755); err != nil {
		return err
	}
	for _, domain := range domains {
		path := filepath.Join(s.resolvers, domain)
		entry, ok := s.journal.Resolvers[domain]
		if !ok {
			before, err := readImage(path)
			if err != nil {
				return err
			}
			entry.Before = before
		}
		entry.Written = []byte(fmt.Sprintf("# kubevpn local API gateway\ndomain %s\nnameserver 127.0.0.1\nport %d\noptions timeout:1\n", domain, port))
		s.journal.Resolvers[domain] = entry
		if err := s.save(); err != nil {
			return err
		}
		image := resolverWrittenImage(entry)
		if err := replaceImage(path, image); err != nil {
			return err
		}
	}
	return s.UpdateHosts(hostnames)
}

func (s *SystemState) UpdateHosts(hostnames []string) error {
	current, err := readImage(s.hosts)
	if err != nil {
		return err
	}
	if !current.Exists {
		return errors.New("hosts file disappeared; refusing to recreate external state")
	}
	if !bytes.Equal(current.Data, s.journal.ExpectedHosts) {
		s.journal.ExactHosts = false
	}
	data, removed := stripLoopbackHosts(current.Data, hostnames)
	if len(removed) == 0 {
		return nil
	}
	s.journal.Removed = append(s.journal.Removed, removed...)
	s.journal.ExpectedHosts = data
	if err := s.save(); err != nil {
		return err
	}
	current.Data = data
	return replaceImage(s.hosts, current)
}

// Restore preserves concurrent edits rather than replacing an external change
// with the initial backup. Unrestored resolver files leave the journal intact.
func (s *SystemState) Restore() error {
	var errs []error
	domains := make([]string, 0, len(s.journal.Resolvers))
	for domain := range s.journal.Resolvers {
		domains = append(domains, domain)
	}
	sort.Strings(domains)
	for _, domain := range domains {
		if !validDomain(domain) {
			errs = append(errs, fmt.Errorf("invalid journal domain %q", domain))
			continue
		}
		entry := s.journal.Resolvers[domain]
		path := filepath.Join(s.resolvers, domain)
		current, err := readImage(path)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if sameImage(current, entry.Before) {
			continue
		}
		if !sameImage(current, resolverWrittenImage(entry)) {
			errs = append(errs, fmt.Errorf("resolver %s changed externally; left untouched", domain))
			continue
		}
		if err = replaceImage(path, entry.Before); err != nil {
			errs = append(errs, err)
		}
	}
	current, err := readImage(s.hosts)
	if err != nil {
		return errors.Join(append(errs, err)...)
	}
	if !current.Exists {
		return errors.Join(append(errs, errors.New("hosts file disappeared; recovery journal retained"))...)
	}
	if s.journal.ExactHosts && bytes.Equal(current.Data, s.journal.ExpectedHosts) && current.Mode == s.journal.Hosts.Mode && current.UID == s.journal.Hosts.UID && current.GID == s.journal.Hosts.GID {
		if err = replaceImage(s.hosts, s.journal.Hosts); err != nil {
			errs = append(errs, err)
		}
	} else {
		current.Data = restoreRemovedHosts(current.Data, s.journal.Removed)
		if err = replaceImage(s.hosts, current); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (s *SystemState) Finish() error { return os.Remove(filepath.Join(s.dir, "journal.json")) }
func (s *SystemState) Close() error {
	if s.lock == nil {
		return nil
	}
	_ = syscall.Flock(int(s.lock.Fd()), syscall.LOCK_UN)
	err := s.lock.Close()
	s.lock = nil
	return err
}

func validDomain(s string) bool {
	if s == "" || strings.HasPrefix(s, ".") || strings.HasSuffix(s, ".") || strings.Contains(s, "..") || !strings.Contains(s, ".") {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '.' || c == '-') {
			return false
		}
	}
	return true
}

func readImage(path string) (fileImage, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return fileImage{}, nil
	}
	if err != nil {
		return fileImage{}, err
	}
	if !info.Mode().IsRegular() {
		return fileImage{}, fmt.Errorf("refusing nonregular file %s", path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return fileImage{}, err
	}
	stat := info.Sys().(*syscall.Stat_t)
	return fileImage{Exists: true, Data: data, Mode: uint32(info.Mode().Perm()), UID: int(stat.Uid), GID: int(stat.Gid)}, nil
}

func replaceImage(path string, image fileImage) error {
	if !image.Exists {
		err := os.Remove(path)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if _, err := readImage(path); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".kubevpn-gateway-")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	defer f.Close()
	if err = f.Chmod(os.FileMode(image.Mode)); err != nil {
		return err
	}
	if image.UID != os.Geteuid() || image.GID != os.Getegid() {
		if err = f.Chown(image.UID, image.GID); err != nil {
			return err
		}
	}
	if _, err = f.Write(image.Data); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}

func stripLoopbackHosts(data []byte, names []string) ([]byte, []hostMapping) {
	selected := map[string]bool{}
	for _, n := range names {
		selected[strings.ToLower(strings.TrimSuffix(n, "."))] = true
	}
	var result []string
	var removed []hostMapping
	for _, line := range strings.Split(string(data), "\n") {
		body, comment, _ := strings.Cut(line, "#")
		fields := strings.Fields(body)
		if len(fields) < 2 {
			result = append(result, line)
			continue
		}
		ip := net.ParseIP(fields[0])
		if ip == nil || ip.To4() == nil || !ip.IsLoopback() {
			result = append(result, line)
			continue
		}
		keep := []string{fields[0]}
		changed := false
		for _, host := range fields[1:] {
			if selected[strings.ToLower(strings.TrimSuffix(host, "."))] {
				removed = append(removed, hostMapping{IP: fields[0], Name: host})
				changed = true
			} else {
				keep = append(keep, host)
			}
		}
		if !changed {
			result = append(result, line)
			continue
		}
		if len(keep) > 1 {
			updated := strings.Join(keep, "\t")
			if comment != "" {
				updated += " #" + comment
			}
			result = append(result, updated)
		} else if comment != "" {
			result = append(result, "#"+comment)
		}
	}
	return []byte(strings.Join(result, "\n")), removed
}
func restoreRemovedHosts(data []byte, removed []hostMapping) []byte {
	existing := map[string]bool{}
	seen := map[string]bool{}
	for _, line := range strings.Split(string(data), "\n") {
		body, _, _ := strings.Cut(line, "#")
		f := strings.Fields(body)
		if len(f) < 2 {
			continue
		}
		for _, h := range f[1:] {
			existing[strings.ToLower(strings.TrimSuffix(h, "."))] = true
		}
	}
	result := string(data)

	for _, entry := range removed {
		name := strings.ToLower(strings.TrimSuffix(entry.Name, "."))
		key := entry.IP + " " + name
		if !existing[name] && !seen[key] {
			if result != "" && !strings.HasSuffix(result, "\n") {
				result += "\n"
			}
			result += entry.IP + "\t" + entry.Name + "\n"
			seen[key] = true
		}
	}
	return []byte(result)
}

func sameImage(a, b fileImage) bool {
	return a.Exists == b.Exists && bytes.Equal(a.Data, b.Data) && a.Mode == b.Mode && a.UID == b.UID && a.GID == b.GID
}
func resolverWrittenImage(entry resolverImage) fileImage {
	image := entry.Before
	image.Exists = true
	image.Data = entry.Written
	if !entry.Before.Exists {
		image.Mode = 0644
		image.UID = os.Geteuid()
		image.GID = os.Getegid()
	}
	return image
}

// PruneAbsentPending only forgets unconfirmed operations when no local interface
// has that address. It never deletes an address whose ownership is uncertain.
func (s *SystemState) PruneAbsentPending() error {
	interfaces, err := net.Interfaces()
	if err != nil {
		return err
	}
	present := map[string]bool{}
	for _, iface := range interfaces {
		addresses, err := iface.Addrs()
		if err != nil {
			return err
		}
		for _, a := range addresses {
			ip, _, err := net.ParseCIDR(a.String())
			if err == nil {
				present[ip.String()] = true
			}
		}
	}
	pending := s.journal.PendingIPs[:0]
	for _, ip := range s.journal.PendingIPs {
		if present[ip] {
			pending = append(pending, ip)
		}
	}
	s.journal.PendingIPs = pending
	return s.save()
}
