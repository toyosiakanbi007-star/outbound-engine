'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { cn } from '@/lib/utils/cn';
import {
  LayoutDashboard, Users, Search, Building2, ShieldCheck,
  ListTodo, ScrollText, Settings, Activity, Sparkles,
} from 'lucide-react';

const NAV_ITEMS = [
  { href: '/', label: 'Dashboard', icon: LayoutDashboard },
  { href: '/clients', label: 'Clients', icon: Users },
  { href: '/onboarding', label: 'Onboarding', icon: Sparkles },
  { href: '/discovery-runs', label: 'Discovery', icon: Search },
  { href: '/companies', label: 'Companies', icon: Building2 },
  { href: '/prequal-runs', label: 'Prequal', icon: ShieldCheck },
  { href: '/queue', label: 'Queue', icon: ListTodo },
  { href: '/logs', label: 'Logs', icon: ScrollText },
  { href: '/settings', label: 'Settings', icon: Settings },
];

export function Sidebar() {
  const pathname = usePathname();

  return (
    <aside className="w-56 flex-shrink-0 border-r border-border bg-card flex flex-col">
      {/* Logo / brand */}
      <div className="h-14 flex items-center gap-2 px-4 border-b border-border">
        <Activity className="w-5 h-5 text-primary" />
        <span className="font-semibold text-sm tracking-tight">Outbound Engine</span>
      </div>

      {/* Nav links */}
      <nav className="flex-1 py-3 px-2 space-y-0.5 overflow-y-auto">
        {NAV_ITEMS.map(({ href, label, icon: Icon }) => {
          const isActive = href === '/' ? pathname === '/' : pathname.startsWith(href);
          return (
            <Link
              key={href}
              href={href}
              className={cn(
                'flex items-center gap-2.5 px-3 py-2 rounded-md text-sm transition-colors',
                isActive
                  ? 'bg-primary/10 text-primary font-medium'
                  : 'text-muted-foreground hover:text-foreground hover:bg-muted',
              )}
            >
              <Icon className="w-4 h-4 flex-shrink-0" />
              {label}
            </Link>
          );
        })}
      </nav>

      {/* Footer */}
      <div className="px-4 py-3 border-t border-border">
        <p className="text-xs text-muted-foreground">v0.3.0 · Control Panel</p>
      </div>
    </aside>
  );
}
